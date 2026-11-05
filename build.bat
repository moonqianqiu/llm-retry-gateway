@echo off
setlocal EnableExtensions
chcp 65001 >nul
pushd "%~dp0"

go vet ./... || goto :fail
go build -trimpath -ldflags "-s -w" -o llm-retry-gateway.exe || goto :fail

echo 构建完成: llm-retry-gateway
pause
popd
exit /b 0

:fail
echo 构建失败
pause
popd
exit /b 1
