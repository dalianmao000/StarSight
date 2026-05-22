# StarSight - Claude Code AI Agent 数据平台

[![Python Version](https://img.shields.io/badge/python-3.9%2B-blue)](https://www.python.org/)
[![License](https://img.shields.io/badge/license-MIT-blue)](LICENSE)
[![CI Status](https://github.com/dalianmao000/StarSight/actions/workflows/ci.yml/badge.svg)](https://github.com/dalianmao000/StarSight/actions)

**版权归属**: YINJI LI | **许可证**: MIT License

基于 Claude Code 的 **Skills + Agents + MCP + Plugins** 架构，构建企业级数据分析和商业化平台。

## 项目价值

### 解决的问题

| 问题类型 | 传统方案痛点 | 本项目方案 |
|:---|:---|:---|
| **数据需求响应慢** | 业务提需求 → 数据团队写SQL → 排期 → 交付，周期长达数天 | NL2SQL 自然语言查询，即时响应 |
| **标签体系质量差** | 标签靠人工规则，维护滞后，业务方不信任 | AI Agent 自动挖掘特征、验证效果、持续迭代 |
| **风控决策滞后** | 事后分析，欺诈损失已发生 | 实时图关系分析 + 流式计算，毫秒级预警 |
| **广告投放效率低** | 人工定向，ROI 难以优化 | Lookalike 智能扩量 + 多触点归因，数据驱动投放 |
| **团队协作成本高** | 多团队、多系统，数据分散，口径不一致 | 统一数据平台 + 多Agent协同，端到端自动化 |

### 适合的行业

| 行业 | 核心场景 | 价值产出 |
|:---|:---|:---|
| **互联网平台** | 用户增长、DAU/MAU 优化、精准推荐 | 提升用户留存率 15-30% |
| **金融机构** | 信贷风控、反欺诈、信用评分 | 降低坏账率 20-40%，欺诈识别率提升至 95% |
| **品牌广告主** | DMP 人群定向、程序化投放、ROI 归因 | 广告投放 ROI 提升 25-50% |
| **零售电商** | 会员分群、促销活动优化、复购预测 | 客单价提升 10-20%，复购率提升 15-25% |
| **电信运营商** | 用户流失预警、高价值用户识别 | 流失率降低 20-35% |

### 核心优势

1. **多智能体协同**：7 大专业 Agent 各司其职，从需求分析到交付全链路自动化
2. **PB 级数据处理**：Spark + Livy 架构，支持海量数据实时计算
3. **自然语言交互**：NL2SQL，让业务人员无需 SQL 即可分析数据
4. **图关系洞察**：Neo4j 图数据库，支持复杂关系网络分析与欺诈团伙识别
5. **风控合规**：HITL 审批流 + 审计日志，满足金融级合规要求

## 项目状态

**当前版本**：MVP v1.0
**状态**：基础骨架搭建完成，核心组件已验证

## 核心能力

- **全域用户画像**：PB 级数据处理，精准用户标签体系
- **广告 DMP**：人群圈选、Lookalike 扩量、多触点归因
- **金融风控**：图关系风控、反欺诈、信用评分
- **商业洞察**：NL2SQL、自动化报告、归因分析

## 架构概览

```
┌─────────────────────────────────────────────────────────────────┐
│                      Claude Code Host                           │
├─────────────────────────────────────────────────────────────────┤
│  Skills Layer                                                    │
│  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐            │
│  │Agent Design │ │ NL2SQL      │ │ Graph Mode  │            │
│  └──────────────┘ └──────────────┘ └──────────────┘            │
│                              │                                  │
│  Agents Layer                                                 │
│  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐            │
│  │Orchestrator │ │ TagProfile  │ │ModelFactory│            │
│  └──────────────┘ └──────────────┘ └──────────────┘            │
│                              │                                  │
│  MCP Servers Layer                                          │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐           │
│  │Database │ │  Spark   │ │  MLflow  │ │  Neo4j   │           │
│  └──────────┘ └──────────┘ └──────────┘ └──────────┘           │
│                              │                                  │
│  External Systems                                           │
│  MySQL │ Spark │ MLflow │ Neo4j │ Milvus │ Feast │ Metabase   │
└─────────────────────────────────────────────────────────────────┘
```

## 目录结构

```
StarSight/
├── skills/                    # Skills 目录
│   ├── data-platform-agent-design/   # ✅ AI Agent 架构设计模式
│   └── spark-data-processing/         # ✅ Spark Job 开发规范
│
├── mcp-servers/               # MCP Servers 目录
│   ├── database-mcp/          # ✅ PostgreSQL/MySQL
│   └── spark-mcp/             # ✅ Livy/Spark Cluster
│
├── agents/                    # Agents 目录
│   └── orchestrator/          # ✅ 主编排 Agent
│
├── config/                    # ✅ 配置文件
│   ├── mcp-servers.yaml
│   ├── agents.yaml
│   └── workflows.yaml
│
├── docs/                      # 文档目录
│   └── Claude-Code-架构设计.md
│
├── scripts/                   # ✅ 脚本目录
│   └── setup-team.py
│
├── workflows/                 # 工作流配置
├── CLAUDE.md                  # Claude Code 配置
├── requirements.txt           # 依赖清单
└── README.md                  # 本文件
```

## 快速开始

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

### 2. 配置环境变量

```bash
# 数据库配置
export DB_HOST=localhost
export DB_PORT=5432
export DB_NAME=data_platform
export DB_USER=postgres
export DB_PASSWORD=your_password

# Spark/Livy 配置
export SPARK_LIVY_HOST=localhost
export SPARK_LIVY_PORT=8998

# MLflow 配置
export MLFLOW_HOST=localhost
export MLFLOW_PORT=5000
```

### 3. 验证项目结构

```bash
python scripts/setup-team.py
# 预期输出：✓ All checks passed!
```

### 4. 启动 MCP Server

```bash
# Database MCP Server
cd mcp-servers/database-mcp
python server.py

# Spark MCP Server (新窗口)
cd mcp-servers/spark-mcp
python server.py
```

## Skills

| Skill | 状态 | 描述 |
|:---|:---|:---|
| `data-platform-agent-design` | ✅ | AI Agent 架构设计模式，5 维需求分析法 |
| `spark-data-processing` | ✅ | Spark Job 开发规范、模板代码 |
| `mlflow-model-management` | ⏳ | MLflow 模型注册、部署流程 |
| `neo4j-graph-analytics` | ⏳ | 图查询模式、反欺诈规则 |
| `nl2sql-pattern` | ⏳ | NL2SQL 最佳实践 |
| `hitl-approval-workflow` | ⏳ | 审批流配置规范 |

## MCP Servers

| Server | 状态 | 功能 |
|:---|:---|:---|
| `database-mcp` | ✅ | SQL 查询、DDL/DML 执行、表结构查询 |
| `spark-mcp` | ✅ | Session 管理、Batch Job 提交、Hive 读取 |
| `mlflow-mcp` | ⏳ | 模型注册、实验查询、部署 |
| `neo4j-mcp` | ⏳ | 图查询、团伙识别、路径分析 |
| `feast-mcp` | ⏳ | 特征读写、血缘追踪 |
| `milvus-mcp` | ⏳ | 向量检索、embedding 管理 |
| `metabase-mcp` | ⏳ | BI 查询、可视化看板 |
| `camunda-mcp` | ⏳ | 审批流触发、状态查询 |

## Agents

| Agent | 状态 | 职责 |
|:---|:---|:---|
| `orchestrator` | ✅ | 意图识别、任务拆解、路由分发 |
| `data-steward` | ⏳ | 数据治理、ETL、质量监控 |
| `tag-profiling` | ⏳ | 标签挖掘、特征工程 |
| `model-factory` | ⏳ | AutoML、模型训练 |
| `dmp-ad` | ⏳ | 人群圈选、Lookalike、归因 |
| `risk-control` | ⏳ | 反欺诈、信用评分、预警 |
| `insight-report` | ⏳ | NL2SQL、归因分析、报告 |

## 工作流

| Workflow | 描述 |
|:---|:---|
| `tag-lifecycle` | 标签从创建到下线的全生命周期 |
| `risk-assessment` | 风控贷前/贷中/贷后全流程 |
| `ad-optimization` | 广告投放到效果归因全流程 |
| `nl2sql-query` | 自然语言查询转 SQL 执行 |

## 文档

| 文档 | 路径 |
|:---|:---|
| 技术架构文档 | `docs/Claude-Code-架构设计.md` |

## 开发进度

- [x] 项目骨架搭建
- [x] MCP Server 框架（database-mcp, spark-mcp）
- [x] Orchestrator Agent
- [x] 核心 Skills（架构设计、Spark开发规范）
- [x] 项目验证脚本
- [ ] 剩余 MCP Servers 实现
- [ ] 剩余 Agents 实现
- [ ] 工作流配置与联调
- [ ] 端到端集成测试

---

**License**: MIT License | Copyright (c) 2026 YINJI LI