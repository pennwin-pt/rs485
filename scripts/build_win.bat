@echo off
chcp 65001 >nul
setlocal

set APP_NAME=rs485-debugger

rem 如果是在 scripts 目录下执行的（比如双击 scripts\build_win.bat 或 cd scripts 后运行），
rem go build 需要的是项目根目录（里面有 go.mod 和 cmd\testtool），所以先切到上一级目录，
rem 编译完再切回来，不影响脚本执行完之后所在的目录。
set "PUSHED=0"
for %%I in ("%CD%") do set "CURR_DIR_NAME=%%~nxI"
if /I "%CURR_DIR_NAME%"=="scripts" (
    echo 检测到当前在 scripts 目录下，先切换到上一级目录...
    pushd ..
    set "PUSHED=1"
)

echo 🚀 开始编译 %APP_NAME%.exe ...

set CGO_ENABLED=0
set GOOS=windows
set GOARCH=amd64

go build -o %APP_NAME%.exe .\cmd\testtool
set "BUILD_RESULT=%ERRORLEVEL%"

if "%PUSHED%"=="1" popd

if "%BUILD_RESULT%"=="0" (
    if "%PUSHED%"=="1" (
        echo ✅ 编译成功！输出文件: ..\%APP_NAME%.exe（项目根目录下）
    ) else (
        echo ✅ 编译成功！输出文件: %APP_NAME%.exe
    )
) else (
    echo ❌ 编译失败！
    endlocal
    pause
    exit /b 1
)

endlocal
pause