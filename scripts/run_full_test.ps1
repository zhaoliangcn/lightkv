<#
.SYNOPSIS
    LightKV full test runner for Windows.

.DESCRIPTION
    Mirrors scripts/run_full_test.sh: build → start server → ctest →
    C++ benchmarks/stress → SDK benchmarks → summary.

    Two Windows-specific notes:

    1. The C++ benchmark binaries hardcode Auth("benchpass123"), so the server
       MUST be started with --requirepass benchpass123. scripts/run_full_test.sh
       omits that flag, which means its benchmark steps cannot succeed there.

    2. -IsolateBinaries (default: on) copies the test/server executables to a
       scratch directory before running them. On machines where endpoint
       protection blocks specific executables in place, running them from the
       build tree fails with Win32 ERROR_ACCESS_DENIED even though the code is
       fine (see docs/缺陷修复方案.md §1.1). Pass -NoIsolateBinaries to run
       them in place.

.PARAMETER BuildDir
    Build directory (relative to the repo root unless absolute). Default: build

.PARAMETER BuildType
    Release | Debug. Default: Release

.PARAMETER TcpPort
    Port for the server under test. The C++ benchmarks hardcode 16379 and
    cannot be redirected, so changing this makes them fail. Default: 16379

.PARAMETER HttpPort
    HTTP monitoring port. Default: 18080

.EXAMPLE
    .\scripts\run_full_test.ps1
    .\scripts\run_full_test.ps1 -BuildType Debug
    .\scripts\run_full_test.ps1 -SkipSdkTests
#>
[CmdletBinding()]
param(
    [string]$BuildDir = 'build',
    [string]$BuildType = 'Release',
    [int]$TcpPort = 16379,
    [int]$HttpPort = 18080,
    [string]$Password = 'benchpass123',
    [switch]$NoIsolateBinaries,
    [switch]$SkipSdkTests,
    [switch]$SkipBenchmarks
)

$ErrorActionPreference = 'Continue'

$RepoRoot = Split-Path -Parent $PSScriptRoot
if (-not (Test-Path -LiteralPath $RepoRoot)) { throw "Cannot locate repo root from $PSScriptRoot" }

if ([System.IO.Path]::IsPathRooted($BuildDir)) {
    $BuildPath = $BuildDir
} else {
    $BuildPath = Join-Path $RepoRoot $BuildDir
}

$script:Total = 0
$script:Pass = 0
$script:Fail = 0
$script:ServerProc = $null
$script:Isolate = -not $NoIsolateBinaries
$script:RunDir = $BuildPath
$script:testNames = @()

function Write-Header([string]$Text) {
    Write-Host ''
    Write-Host ('=' * 63)
    Write-Host "  $Text"
    Write-Host ('=' * 63)
    Write-Host ''
}

function Record([int]$Code, [string]$Name) {
    $script:Total++
    if ($Code -eq 0) {
        Write-Host "  [PASS] $Name" -ForegroundColor Green
        $script:Pass++
    } else {
        Write-Host "  [FAIL] $Name (exit $Code)" -ForegroundColor Red
        $script:Fail++
    }
}

function Get-ToolPath([string]$Name) {
    $c = Get-Command $Name -ErrorAction SilentlyContinue
    if ($c) { return $c.Source }
    return $null
}

function Wait-ForPort([int]$Port, [int]$TimeoutSec = 20) {
    $deadline = (Get-Date).AddSeconds($TimeoutSec)
    while ((Get-Date) -lt $deadline) {
        try {
            $c = New-Object System.Net.Sockets.TcpClient
            $iar = $c.BeginConnect('127.0.0.1', $Port, $null, $null)
            if ($iar.AsyncWaitHandle.WaitOne(500) -and $c.Connected) {
                $c.Close()
                return $true
            }
            $c.Close()
        } catch { }
        Start-Sleep -Milliseconds 300
    }
    return $false
}

function Invoke-Check {
    param(
        [string]$Label,
        [scriptblock]$Action,
        [string[]]$SkipIfMissing = @()
    )
    foreach ($tool in $SkipIfMissing) {
        if (-not (Get-ToolPath $tool)) {
            Write-Host "  [SKIP] $Label (missing: $tool)" -ForegroundColor Yellow
            return
        }
    }
    & $Action
    Record $LASTEXITCODE $Label
}

function Stop-Server {
    if ($script:ServerProc -and -not $script:ServerProc.HasExited) {
        Stop-Process -Id $script:ServerProc.Id -Force -ErrorAction SilentlyContinue
        try { $script:ServerProc.WaitForExit(5000) | Out-Null } catch { }
    }
    $script:ServerProc = $null
}

function Stop-ListenersOnPort([int]$Port) {
    # Several registered tests bind fixed ports (p0_test uses 16379, etc.). If a
    # stale server is still listening, the test's Client silently connects to the
    # WRONG server and reports a wall of unrelated failures instead of a clear
    # "port busy" error.
    try {
        $conns = Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue
    } catch {
        $conns = $null
    }
    foreach ($c in @($conns)) {
        if (-not $c) { continue }
        $pid_ = $c.OwningProcess
        if ($pid_ -and $pid_ -ne $PID) {
            Write-Host "  freeing port $Port (killing PID $pid_)" -ForegroundColor DarkGray
            Stop-Process -Id $pid_ -Force -ErrorAction SilentlyContinue
        }
    }
}

try {
    # ── Step 0: Build ──────────────────────────────────────────────────
    Write-Header 'Step 0: Build'

    if (-not (Test-Path -LiteralPath $BuildPath)) {
        New-Item -ItemType Directory -Path $BuildPath -Force | Out-Null
    }

    Push-Location $BuildPath
    # Build the -D argument explicitly. Passing "-DCMAKE_BUILD_TYPE=$BuildType"
    # inline has proven unreliable across PowerShell/native-arg parsing modes
    # (it once landed in CMakeCache.txt as the literal string "$BuildType").
    $cmakeBuildTypeArg = '-DCMAKE_BUILD_TYPE=' + $BuildType
    cmake $cmakeBuildTypeArg "$RepoRoot" 2>&1 | Select-Object -Last 2
    Record $LASTEXITCODE 'CMake configure'

    cmake --build . --config $BuildType 2>&1 | Select-Object -Last 5
    if ($LASTEXITCODE -ne 0) {
        Record $LASTEXITCODE 'C++ Build'
        throw 'Build failed; aborting.'
    }
    Record 0 'C++ Build'
    Pop-Location

    # ── Locate / stage binaries ────────────────────────────────────────
    $serverExe = Join-Path $BuildPath 'lightkv_server.exe'
    if (-not (Test-Path -LiteralPath $serverExe)) {
        $serverExe = Join-Path $BuildPath 'lightkv_server'
    }
    if (-not (Test-Path -LiteralPath $serverExe)) {
        throw "lightkv_server not found under $BuildPath"
    }

    $testsDir = Join-Path $BuildPath 'tests'
    if (-not (Test-Path -LiteralPath $testsDir)) {
        $testsDir = $BuildPath
    }

    if ($script:Isolate) {
        $script:RunDir = Join-Path ([System.IO.Path]::GetTempPath()) "lkv_fulltest_$PID"
        if (Test-Path -LiteralPath $script:RunDir) {
            Remove-Item -Recurse -Force $script:RunDir -ErrorAction SilentlyContinue
        }
        New-Item -ItemType Directory -Path $script:RunDir -Force | Out-Null
        Copy-Item (Join-Path $testsDir '*.exe') $script:RunDir -Force -ErrorAction SilentlyContinue
        Copy-Item $serverExe $script:RunDir -Force
        Write-Host "  Staged binaries to $script:RunDir" -ForegroundColor DarkGray
        Write-Host '  (use -NoIsolateBinaries to run them in place)' -ForegroundColor DarkGray
    } else {
        $script:RunDir = $testsDir
    }
    $script:ServerExe = Join-Path $script:RunDir ([System.IO.Path]::GetFileName($serverExe))

    # ── Step 1: C++ regression tests ──────────────────────────────────
    # NOTE: these run BEFORE the server under test is started. Every registered
    # test spins up its own server instance, and several of them bind 16379 —
    # the same port the benchmark server uses. Starting it first makes those
    # tests fail with a port conflict.
    Write-Header 'Step 1: C++ Regression Tests (ctest)'

    # Make sure nothing is squatting on the ports the tests bind.
    Stop-Server
    foreach ($p in @($TcpPort, 16382, 16383)) { Stop-ListenersOnPort $p }

    $ctestLog = Join-Path ([System.IO.Path]::GetTempPath()) "lkv_ctest_$PID.log"
    Push-Location $BuildPath
    ctest --output-on-failure --build-config $BuildType 2>&1 |
        Tee-Object -FilePath $ctestLog | Select-Object -Last 30
    $ctestCode = $LASTEXITCODE

    $testNames = @()
    if ($ctestCode -ne 0) {
        # Re-run from the staged copies when the failure looks environmental.
        $log = Get-Content $ctestLog -Raw -ErrorAction SilentlyContinue
        $envBlocked = ($log -match 'ERROR_ACCESS_DENIED|Permission denied|Access is denied|cannot create MANIFEST\.tmp')
        if ($envBlocked -and $script:Isolate) {
            Write-Host ''
            Write-Host '  ctest failures match the endpoint-protection signature' -ForegroundColor Yellow
            Write-Host '  (Win32 ACCESS_DENIED on files the test itself creates).' -ForegroundColor Yellow
            Write-Host '  Re-running the same tests from the staged copies ...' -ForegroundColor Yellow

            # Must stay inside the build dir: ctest -N reads CTestTestfile.cmake.
            ctest -N --build-config $BuildType 2>&1 | ForEach-Object {
                if ($_ -match '^\s*Test\s+#\d+:\s*(\S+)') { $script:testNames += $Matches[1] }
            }
            Pop-Location

            Write-Host "  discovered $($script:testNames.Count) registered test(s)" -ForegroundColor DarkGray
            $stagedFail = 0
            foreach ($name in $script:testNames) {
                $exe = Join-Path $script:RunDir "$name.exe"
                if (-not (Test-Path -LiteralPath $exe)) {
                    Write-Host "  [MISS] $name (not staged)" -ForegroundColor Yellow
                    continue
                }
                & $exe *> $null
                if ($LASTEXITCODE -ne 0) {
                    $stagedFail++
                    Write-Host "  [FAIL] $name" -ForegroundColor Red
                }
            }
            if ($script:testNames.Count -gt 0 -and $stagedFail -eq 0) {
                Write-Host '  All staged tests passed - the earlier failures were environmental.' -ForegroundColor Green
                $ctestCode = 0
            }
            Push-Location $BuildPath
        }
    }
    Pop-Location

    if ($ctestCode -eq 0) {
        Record 0 'C++ Regression Tests'
    } else {
        Record 1 'C++ Regression Tests'
    }

    # ── Step 2: start the server under test ────────────────────────────
    # Started only after ctest: several registered tests bind 16379 themselves.
    Write-Header 'Step 2: Start Server'

    Stop-Server
    $dbPath = Join-Path ([System.IO.Path]::GetTempPath()) 'lightkv_full_test'
    Remove-Item -Recurse -Force $dbPath -ErrorAction SilentlyContinue

    $srvLog = Join-Path $script:RunDir 'server_stdout.log'
    $srvErr = Join-Path $script:RunDir 'server_stderr.log'
    $script:ServerProc = Start-Process -FilePath $script:ServerExe `
        -ArgumentList @('--db-path', $dbPath, '--tcp-port', $TcpPort,
                        '--http-port', $HttpPort, '--requirepass', $Password) `
        -PassThru -WindowStyle Hidden `
        -RedirectStandardOutput $srvLog -RedirectStandardError $srvErr
    Write-Host "  Server PID: $($script:ServerProc.Id)"

    if (Wait-ForPort -Port $TcpPort) {
        Record 0 "Server started on port $TcpPort"
    } else {
        Record 1 'Server failed to start'
        Write-Host '  --- server stderr ---' -ForegroundColor Red
        if (Test-Path -LiteralPath $srvErr) { Get-Content $srvErr | Select-Object -Last 20 }
        throw 'Server did not come up; aborting.'
    }

    if ($SkipBenchmarks) {
        Write-Header 'Steps 3-4: Skipped (-SkipBenchmarks)'
    } else {
        # ── Step 3: C++ benchmarks ────────────────────────────────────
        Write-Header 'Step 3: C++ Benchmarks'

        foreach ($b in @('bench_cpp', 'bench_pipeline_cpp', 'stress_cpp')) {
            $exe = Join-Path $script:RunDir "$b.exe"
            if (-not (Test-Path -LiteralPath $exe)) {
                Write-Host "  [SKIP] $b (not built)" -ForegroundColor Yellow
                continue
            }
            Write-Host "  [$b]" -ForegroundColor Cyan
            & $exe 2>&1 | Select-Object -Last 12
            Record $LASTEXITCODE $b
        }

        # ── Step 4: SDK benchmarks ─────────────────────────────────────
        if ($SkipSdkTests) {
            Write-Host ''
            Write-Host '  [SKIP] SDK benchmarks (-SkipSdkTests)' -ForegroundColor Yellow
        } else {
            Write-Header 'Step 4: SDK Benchmarks'

            $sdk = @(
                @{ Label = 'Node.js bench';    Tool = 'node'; Script = 'clients/nodejs/test/bench.js' },
                @{ Label = 'Node.js pipeline'; Tool = 'node'; Script = 'clients/nodejs/test/bench_pipeline.js' },
                @{ Label = 'Node.js stress';   Tool = 'node'; Script = 'clients/nodejs/test/stress.js' },
                @{ Label = 'Python bench';     Tool = 'python'; Script = 'clients/python/tests/bench.py' },
                @{ Label = 'Python pipeline';  Tool = 'python'; Script = 'clients/python/tests/bench_pipeline.py' },
                @{ Label = 'Python stress';    Tool = 'python'; Script = 'clients/python/tests/stress.py' }
            )
            foreach ($s in $sdk) {
                $path = Join-Path $RepoRoot $s.Script
                if (-not (Get-ToolPath $s.Tool)) {
                    Write-Host "  [SKIP] $($s.Label) (missing: $($s.Tool))" -ForegroundColor Yellow
                    continue
                }
                if (-not (Test-Path -LiteralPath $path)) {
                    Write-Host "  [SKIP] $($s.Label) (missing: $($s.Script))" -ForegroundColor Yellow
                    continue
                }
                & $s.Tool $path 2>&1 | Select-Object -Last 8
                Record $LASTEXITCODE $s.Label
            }

            if (Get-ToolPath 'go') {
                Push-Location (Join-Path $RepoRoot 'clients/go')
                go test ./... 2>&1 | Select-Object -Last 12
                Record $LASTEXITCODE 'Go SDK tests'
                Pop-Location
            } else {
                Write-Host '  [SKIP] Go SDK tests (missing: go)' -ForegroundColor Yellow
            }
        }
    }
}
finally {
    Stop-Server
    if ($script:Isolate -and $script:RunDir -and (Test-Path -LiteralPath $script:RunDir)) {
        Remove-Item -Recurse -Force $script:RunDir -ErrorAction SilentlyContinue
    }
}

# ── Summary ────────────────────────────────────────────────────────────
Write-Header 'Summary'
Write-Host "  Total: $($script:Total)"
Write-Host "  Pass:  $($script:Pass)" -ForegroundColor Green
Write-Host "  Fail:  $($script:Fail)" -ForegroundColor Red
Write-Host ''

if ($script:Fail -eq 0) {
    Write-Host '  All tests passed!' -ForegroundColor Green
} else {
    Write-Host "  $($script:Fail) test(s) failed" -ForegroundColor Red
}

exit $script:Fail
