#!/bin/bash
# Tushare 数据每日更新脚本

PROJECT_DIR="/Users/candyhu/a-quant-system"
PYTHON_PATH="/usr/bin/python3"
LOG_DIR="$PROJECT_DIR/logs"

# 创建日志目录
mkdir -p "$LOG_DIR"

echo "=================================================="
echo "Tushare 数据每日更新"
echo "=================================================="
echo "时间: $(date '+%Y-%m-%d %H:%M:%S')"
echo "=================================================="

# 执行数据更新
cd $PROJECT_DIR && $PYTHON_PATH scripts/download_tushare_data.py --mode update >> "$LOG_DIR/tushare_update.log" 2>&1

echo "=================================================="
echo "更新完成: $(date '+%Y-%m-%d %H:%M:%S')"
echo "日志文件: $LOG_DIR/tushare_update.log"
echo "=================================================="
