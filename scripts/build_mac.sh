#!/bin/bash

APP_NAME="rs485_debugger-mac"

echo "🚀 开始编译 Windows 版本..."

# 编译 ./cmd 目录下的包，生成文件输出到根目录（或指定目录）
CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build -o ${APP_NAME}.exe ./cmd/testtool

if [ $? -eq 0 ]; then
    echo "✅ 编译成功！输出文件: ${APP_NAME}.exe"
else
    echo "❌ 编译失败！"
    exit 1
fi
