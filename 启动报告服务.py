# -*- coding: utf-8 -*-
"""
选品报告生成器 - 启动入口
双击此文件即可启动服务
"""
import subprocess
import sys
import os
import webbrowser
import time

def main():
    # 获取当前目录
    app_dir = os.path.dirname(os.path.abspath(__file__))

    # 启动Flask服务
    print("=" * 50)
    print("  选品报告生成器 启动中...")
    print("=" * 50)

    # 启动后端服务
    server_script = os.path.join(app_dir, "app.py")
    if not os.path.exists(server_script):
        print("错误：找不到 app.py 文件！")
        input("按回车键退出...")
        return

    print("\n正在启动Web服务...")

    # 使用 subprocess 启动服务，不显示黑窗口
    startupinfo = subprocess.STARTUPINFO()
    startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
    startupinfo.wShowWindow = subprocess.SW_HIDE

    process = subprocess.Popen(
        [sys.executable, server_script],
        cwd=app_dir,
        startupinfo=startupinfo,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )

    # 等待服务启动
    print("等待服务启动...")
    time.sleep(3)

    # 打开浏览器
    url = "http://127.0.0.1:5000"
    print(f"\n服务已启动！正在打开浏览器...")
    print(f"网址：{url}")
    print("\n" + "=" * 50)
    print("  使用说明：")
    print("  1. 上传BSR数据文件和评论文件")
    print("  2. 点击「生成报告」按钮")
    print("  3. 等待报告生成后下载")
    print("=" * 50)
    print("\n关闭时请直接关闭此窗口，或按 Ctrl+C")

    webbrowser.open(url)

    # 保持运行
    try:
        process.wait()
    except KeyboardInterrupt:
        print("\n正在停止服务...")
        process.terminate()
        process.wait()
        print("服务已停止。")

if __name__ == "__main__":
    main()
