#!/bin/bash
# 定时任务设置脚本

PROJECT_DIR="/Users/candyhu/a-quant-system"
PYTHON_PATH="/usr/bin/python3"

# 创建日志目录
mkdir -p "$PROJECT_DIR/logs"

# 添加crontab任务
# 每天下午3点半运行（周一到周五）
# 30 15 * * 1-5 → 30分 15时（下午3点半） 周一到周五
echo "# A股量化系统 - 每日信号生成" | crontab -
echo "30 15 * * 1-5 cd $PROJECT_DIR && $PYTHON_PATH scripts/daily_signals.py >> logs/scheduler.log 2>&1" | crontab -

echo ""
echo "✅ 定时任务已设置"
echo "   运行时间: 每周一到周五 下午3点半"
echo "   日志文件: $PROJECT_DIR/logs/scheduler.log"
echo ""

# 查看当前定时任务
echo "=== 当前定时任务 ==="
crontab -l

echo ""
echo "📝 提示:"
echo "   - 每天15:30自动运行（收盘后30分钟，数据已稳定）"
echo "   - 查看日志: tail -f $PROJECT_DIR/logs/scheduler.log"
echo "   - 编辑任务: crontab -e"
echo "   - 删除任务: crontab -e（删除所有行）"
