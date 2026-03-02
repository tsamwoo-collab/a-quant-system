# A股量化信号系统 - 快速开始

## 🚀 快速启动

```bash
/Users/candyhu/a-quant-system/quick_start.sh
```

或手动执行：

```bash
cd /Users/candyhu/a-quant-system/dashboard
streamlit run app_allinone.py --server.port 8501
```

然后访问：**http://localhost:8501**

---

## 📊 系统功能

### 核心特性
- ✅ **宏观-中观-微观**三层因子分析
- ✅ **持仓追踪**与信号对比
- ✅ **动能归因**分析（哪个层级是主驱动力）
- ✅ **跨越雷池**交易信号（只在临界突破时触发）

### 跨越雷池逻辑
| 触发条件 | 操作 |
|---------|------|
| 昨天 < 0.30 → 今天 ≥ 0.30 | 🚨 买入 |
| 昨天 > -0.30 → 今天 ≤ -0.30 | ⚠️ 清仓 |

### 系统忽略
- ❌ 昨天就高、今天仍高的（无跨越）
- ❌ 一直在谷底的（无跨越）

---

## 📁 项目结构

```
a-quant-system/
├── dashboard/
│   └── app_allinone.py       # ⭐ 单文件版本（推荐）
├── factors/                   # 因子模块
│   ├── macro.py              # 宏观因子
│   ├── meso.py               # 中观因子
│   └── micro.py              # 微观因子
├── config/                    # 配置
│   └── settings.py
├── PROJECT_SUMMARY.md        # 完整项目总结
├── quick_start.sh            # ⭐ 快速启动脚本
└── requirements.txt
```

---

## 🔧 明天继续

```bash
# 方式1：使用快速启动脚本（最简单）
/Users/candyhu/a-quant-system/quick_start.sh

# 方式2：手动启动
cd /Users/candyhu/a-quant-system/dashboard
streamlit run app_allinone.py --server.port 8501
```

---

## 📖 完整文档

查看 `PROJECT_SUMMARY.md` 了解：
- 详细功能说明
- 技术架构
- 后续扩展方向
- 问题排查

---

**创建日期**：2026-02-27
**版本**：v1.0 MVP
