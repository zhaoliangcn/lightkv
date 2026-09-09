# LightKV Windows One-Click Build Script
# Usage:  .\build.ps1 [Release|Debug]
# Default: Release

param(
    [ValidateSet("Release","Debug")]
    [string]$BuildType = "Release"
)

$ErrorActionPreference = "Stop"
$ProjectDir = $PSScriptRoot
$BuildDir    = Join-Path $ProjectDir "build"

Write-Host ""
Write-Host "===================================================="
Write-Host " LightKV Windows Build  [$BuildType]"
Write-Host "===================================================="
Write-Host ""

# ── 1. Check cmake ──
if (-not (Get-Command cmake -ErrorAction SilentlyContinue)) {
    Write-Host "[ERROR] cmake not found. Install CMake 3.16+ and add to PATH." -ForegroundColor Red
    exit 1
}
cmake --version | Select-String "cmake version"

# ── 2. Detect compiler & generator ──
$CxxFlag = ""
$CFlag   = ""
$UseNinja = $true

if (Get-Command ninja -ErrorAction SilentlyContinue) {
    $NinjaVer = (ninja --version 2>$null).Trim()
    Write-Host "  ninja $NinjaVer"
} else {
    Write-Host "  [INFO] ninja not found, using NMake Makefiles." -ForegroundColor Yellow
    $UseNinja = $false
}

if (Get-Command clang++ -ErrorAction SilentlyContinue) {
    Write-Host "  Compiler: clang++"
    $CxxFlag = "-DCMAKE_CXX_COMPILER=clang++"
    $CFlag   = "-DCMAKE_C_COMPILER=clang"
} elseif (Get-Command g++ -ErrorAction SilentlyContinue) {
    Write-Host "  Compiler: g++"
    $CxxFlag = "-DCMAKE_CXX_COMPILER=g++"
    $CFlag   = "-DCMAKE_C_COMPILER=gcc"
} else {
    Write-Host "  Compiler: MSVC (default)"
}

# ── 3. Prepare build directory ──
Write-Host ""
Write-Host "[1/3] Preparing build directory..."
if (Test-Path $BuildDir) { Remove-Item $BuildDir -Recurse -Force }
New-Item -ItemType Directory -Path $BuildDir -Force | Out-Null

# ── 4. CMake configure ──
Write-Host "[2/3] Configuring with CMake..."
Write-Host ""

$Generator = if ($UseNinja) { "Ninja" } else { "NMake Makefiles" }

$cmakeArgs = @(
    "-S", $ProjectDir,
    "-B", $BuildDir,
    "-G", $Generator,
    "-DCMAKE_BUILD_TYPE=$BuildType"
)
if ($CxxFlag) { $cmakeArgs += $CxxFlag }
if ($CFlag)   { $cmakeArgs += $CFlag }

& cmake @cmakeArgs
if ($LASTEXITCODE -ne 0) {
    Write-Host ""
    Write-Host "[ERROR] CMake configuration failed." -ForegroundColor Red
    exit 1
}

# ── 5. Build ──
Write-Host ""
Write-Host "[3/3] Building..."
Write-Host ""

if ($UseNinja) {
    ninja -C $BuildDir -j $env:NUMBER_OF_PROCESSORS
} else {
    cmake --build $BuildDir --config $BuildType
}
if ($LASTEXITCODE -ne 0) {
    Write-Host ""
    Write-Host "[ERROR] Build failed." -ForegroundColor Red
    exit 1
}

# ── 6. Summary ──
Write-Host ""
Write-Host "===================================================="
Write-Host " Build succeeded!  [$BuildType]"
Write-Host "===================================================="
Write-Host ""
Write-Host "  Output:"
$lib = Join-Path $BuildDir "lightkv.lib"
$server = Join-Path $BuildDir "lightkv_server.exe"
$migrate = Join-Path $BuildDir "lightkv_migrate.exe"
if (Test-Path $lib)    { Write-Host "    lib:   $lib" }
if (Test-Path $server) { Write-Host "    bin:   $server" }
if (Test-Path $migrate){ Write-Host "    bin:   $migrate" }
Write-Host ""
