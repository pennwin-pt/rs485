#!/bin/bash

APP_NAME="rs485-debugger"

# 如果是在 scripts 目录下执行的（比如 ./scripts/build_mac.sh 或 cd scripts && ./build_mac.sh），
# go build 需要的是项目根目录（里面有 go.mod 和 ./cmd/testtool），所以先切到上一级目录，
# 编译完再切回来，不影响脚本执行完之后所在的目录。
PUSHED=0
if [ "$(basename "$PWD")" = "scripts" ]; then
    echo "检测到当前在 scripts 目录下，先切换到上一级目录..."
    pushd .. > /dev/null
    PUSHED=1
fi

echo "🚀 开始编译 Windows 版本..."

# 编译 ./cmd 目录下的包，生成文件输出到根目录（或指定目录）
CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build -o ${APP_NAME}.exe ./cmd/testtool

BUILD_RESULT=$?

if [ "$PUSHED" -eq 1 ]; then
    popd > /dev/null
fi

if [ $BUILD_RESULT -eq 0 ]; then
    if [ "$PUSHED" -eq 1 ]; then
        echo "✅ 编译成功！输出文件: ../${APP_NAME}.exe（项目根目录下）"
    else
        echo "✅ 编译成功！输出文件: ${APP_NAME}.exe"
    fi
else
    echo "❌ 编译失败！"
    exit 1
fi