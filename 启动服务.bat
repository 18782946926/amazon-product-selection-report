@echo off
chcp 65001 >nul
title 选品报告生成系统

echo ========================================
echo    选品报告生成系统 启动中...
echo ========================================
echo.

cd /d "%~dp0"

echo [1/2] 检查Python环境...
C:\Users\18782\.workbuddy\binaries\python\envs\default\Scripts\python.exe -c "import flask" 2>nul
if errorlevel 1 (
    echo [错误] Flask未安装，正在安装...
    C:\Users\18782\.workbuddy\binaries\python\envs\default\Scripts\pip.exe install flask pandas openpyxl werkzeug
    echo.
)

echo [2/2] 启动Web服务...
echo.
echo ========================================
echo    服务已启动！
echo    请在浏览器打开: http://localhost:5000
echo ========================================
echo.
echo 按 Ctrl+C 停止服务
echo.

C:\Users\18782\.workbuddy\binaries\python\envs\default\Scripts\python.exe app.py

pause
