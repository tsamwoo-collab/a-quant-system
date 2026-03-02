#!/bin/bash
# A股量化信号系统 - 快速启动脚本
echo "🚀 启动 A股量化信号系统..."

PROJECT_ROOT="/Users/candyhu/a-quant-system"
export PYTHONPATH="$PROJECT_ROOT:$PYTHONPATH"

cd "$PROJECT_ROOT"
pkill -9 -f streamlit 2>/dev/null

cd dashboard
STREAMLIT_SERVER_HEADLESS=true python3 -m streamlit run app_allinone.py --server.port 8501 --server.headless true
