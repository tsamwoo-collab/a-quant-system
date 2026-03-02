#!/bin/bash
# A股量化信号系统 - 快速启动脚本

echo "========================================"
echo "  A股量化信号系统 - 启动脚本"
echo "========================================"
echo ""

# 进入项目目录
cd /Users/candyhu/a-quant-system

# 检查streamlit是否已运行
if lsof -i :8501 > /dev/null 2>&1; then
    echo "⚠️  端口8501已被占用，尝试关闭旧进程..."
    pkill -f streamlit 2>/dev/null
    sleep 2
fi

# 启动Dashboard
echo "🚀 启动 Dashboard..."
echo ""
echo "📍 启动命令："
echo "   cd /Users/candyhu/a-quant-system/dashboard"
echo "   streamlit run app_allinone.py --server.port 8501"
echo ""
echo "🌐 访问地址："
echo "   http://localhost:8501"
echo ""
echo "========================================"
echo ""

cd /Users/candyhu/a-quant-system/dashboard
streamlit run app_allinone.py --server.port 8501
