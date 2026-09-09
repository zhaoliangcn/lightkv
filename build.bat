@echo off
setlocal

:: ============================================================
:: LightKV Windows One-Click Build Script
:: Usage:  build.bat [Release|Debug]
:: Default: Release
:: ============================================================

set "BUILD_TYPE=%~1"
if "%BUILD_TYPE%"=="" set "BUILD_TYPE=Release"

set "PROJECT_DIR=%~dp0"
set "BUILD_DIR=%PROJECT_DIR%build"

echo.
echo ====================================================
echo  LightKV Windows Build  [%BUILD_TYPE%]
echo ====================================================
echo.

:: ----------------------------------------------------------
:: 1. Check cmake
:: ----------------------------------------------------------
cmd /c "where cmake >nul 2>&1"
if errorlevel 1 (
    echo [ERROR] cmake not found. Install CMake 3.16+ and add to PATH.
    exit /b 1
)
cmd /c "cmake --version" | findstr /i "cmake version"

:: ----------------------------------------------------------
:: 2. Detect compiler and generator
:: ----------------------------------------------------------
set "CXXFLAGS="
set "CFLAGS="
set "GENERATOR=Ninja"

cmd /c "where ninja >nul 2>&1"
if errorlevel 1 (
    echo [INFO] ninja not found, using NMake Makefiles.
    set "GENERATOR=NMake Makefiles"
) else (
    cmd /c "ninja --version"
)

cmd /c "where clang++ >nul 2>&1"
if not errorlevel 1 (
    echo  Compiler: clang++
    set "CXXFLAGS=-DCMAKE_CXX_COMPILER=clang++"
    set "CFLAGS=-DCMAKE_C_COMPILER=clang"
    goto :configure
)

cmd /c "where g++ >nul 2>&1"
if not errorlevel 1 (
    echo  Compiler: g++
    set "CXXFLAGS=-DCMAKE_CXX_COMPILER=g++"
    set "CFLAGS=-DCMAKE_C_COMPILER=gcc"
    goto :configure
)

echo  Compiler: MSVC ^(default^)

:: ----------------------------------------------------------
:: 3. Clean and create build directory
:: ----------------------------------------------------------
:configure
echo.
echo [1/3] Preparing build directory...
cmd /c "if exist "%BUILD_DIR%" rd /s /q "%BUILD_DIR%""
cmd /c "mkdir "%BUILD_DIR%" 2>nul"

:: ----------------------------------------------------------
:: 4. CMake configure
:: ----------------------------------------------------------
echo [2/3] Configuring with CMake...
echo.

cmd /c "cmake -S "%PROJECT_DIR%" -B "%BUILD_DIR%" -G "%GENERATOR%" %CXXFLAGS% %CFLAGS% -DCMAKE_BUILD_TYPE=%BUILD_TYPE%"
if errorlevel 1 (
    echo.
    echo [ERROR] CMake configuration failed.
    exit /b 1
)

:: ----------------------------------------------------------
:: 5. Build
:: ----------------------------------------------------------
echo.
echo [3/3] Building...
echo.

cmd /c "where ninja >nul 2>&1"
if errorlevel 1 (
    cmd /c "cmake --build "%BUILD_DIR%" --config %BUILD_TYPE%"
) else (
    cmd /c "ninja -C "%BUILD_DIR%" -j%NUMBER_OF_PROCESSORS%"
)

if errorlevel 1 (
    echo.
    echo [ERROR] Build failed.
    exit /b 1
)

:: ----------------------------------------------------------
:: 6. Summary
:: ----------------------------------------------------------
echo.
echo ====================================================
echo  Build succeeded!  [%BUILD_TYPE%]
echo ====================================================
echo.
echo  Output:
cmd /c "if exist "%BUILD_DIR%\lightkv.lib" echo    lib:   %BUILD_DIR%\lightkv.lib"
cmd /c "if exist "%BUILD_DIR%\lightkv_server.exe" echo    bin:   %BUILD_DIR%\lightkv_server.exe"
cmd /c "if exist "%BUILD_DIR%\lightkv_migrate.exe" echo    bin:   %BUILD_DIR%\lightkv_migrate.exe"
echo.

endlocal
