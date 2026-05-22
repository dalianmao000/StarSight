# 数据服务平台 - Claude Code 架构设计文档

**文档版本**：v1.0
**编制日期**：2026-05-22
**编制人**：Claude

---

## 1. 架构概述

### 1.1 设计目标

基于 Claude Code 的 **Skills + Agents + MCP + Plugins** 架构，构建企业级数据服务平台。核心思路：

- **Agent**：作为认知层与编排层，不直接承载计算，而是通过标准化工具调用底层分布式引擎
- **MCP Server**：对接外部数据系统（数据库、Spark、MLflow、Neo4j 等），提供统一工具调用接口
- **Skill**：封装最佳实践与领域知识，确保 Agent 决策质量
- **Plugin**：扩展 Claude Code 的能力边界

### 1.2 架构分层

```
┌─────────────────────────────────────────────────────────────────┐
│                    Claude Code Host Environment                 │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌───────────────────────────────────────────────────────────┐ │
│  │                      Skills Layer                          │ │
│  │  ┌─────────────────┐ ┌─────────────────┐ ┌─────────────┐ │ │
│  │  │data-platform-   │ │ spark-data-     │ │ nl2sql-     │ │ │
│  │  │agent-design     │ │ processing      │ │ pattern     │ │ │
│  │  └─────────────────┘ └─────────────────┘ └─────────────┘ │ │
│  │  ┌─────────────────┐ ┌─────────────────┐ ┌─────────────┐ │ │
│  │  │ mlflow-model-   │ │ neo4j-graph-    │ │ hitl-       │ │ │
│  │  │ management      │ │ analytics       │ │ approval    │ │ │
│  │  └─────────────────┘ └─────────────────┘ └─────────────┘ │ │
│  └───────────────────────────────────────────────────────────┘ │
│                              │                                  │
│  ┌───────────────────────────▼───────────────────────────────┐ │
│  │                      Team: Agents Layer                     │ │
│  │                                                            │ │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐ │ │
│  │  │Orchestrator│  │DataSteward  │  │ TagProfiling Agent  │ │ │
│  │  │  Agent      │  │  Agent      │  │                     │ │ │
│  │  │(Team Lead)  │  │             │  │                     │ │ │
│  │  └─────────────┘  └─────────────┘  └─────────────────────┘ │ │
│  │                                                            │ │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐ │ │
│  │  │ModelFactory │  │DMP/Ad Agent │  │ RiskControl Agent   │ │ │
│  │  │  Agent      │  │             │  │                     │ │ │
│  │  └─────────────┘  └─────────────┘  └─────────────────────┘ │ │
│  │                                                            │ │
│  │  ┌─────────────────────────────────────────────────────────┐ │ │
│  │  │              Insight & Report Agent                      │ │ │
│  │  └─────────────────────────────────────────────────────────┘ │ │
│  └───────────────────────────────────────────────────────────┘ │
│                              │                                  │
│  ┌───────────────────────────▼───────────────────────────────┐ │
│  │                      MCP Servers Layer                      │ │
│  │                                                            │ │
│  │  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐       │ │
│  │  │Database  │ │  Spark   │ │ MLflow   │ │  Neo4j   │       │ │
│  │  │  Server  │ │  Server  │ │  Server  │ │  Server  │       │ │
│  │  │(MySQL/   │ │(Livy/    │ │(Model    │ │(Bolt     │       │ │
│  │  │ PostgreSQL)│ │ REST API)│ │Registry) │ │ Protocol)│       │ │
│  │  └──────────┘ └──────────┘ └──────────┘ └──────────┘       │ │
│  │                                                            │ │
│  │  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐       │ │
│  │  │  Feast   │ │ Milvus   │ │ Metabase │ │ Camunda  │       │ │
│  │  │  Server  │ │  Server  │ │  Server  │ │  Server  │       │ │
│  │  │(gRPC)    │ │(SDK)     │ │ (API)    │ │(REST API)│       │ │
│  │  └──────────┘ └──────────┘ └──────────┘ └──────────┘       │ │
│  └───────────────────────────────────────────────────────────┘ │
│                              │                                  │
│  ┌───────────────────────────▼───────────────────────────────┐ │
│  │                    External Systems                         │ │
│  │  MySQL │ PostgreSQL │ Spark/Hive │ MLflow │ Neo4j │ Milvus │ │
│  └───────────────────────────────────────────────────────────┘ │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## 2. Team 与 Agent 设计

### 2.1 团队结构

```
Team: data-platform-team

Leader: Orchestrator Agent
    │
    ├── DataSteward Agent
    ├── TagProfiling Agent
    ├── ModelFactory Agent
    ├── DMPAd Agent
    ├── RiskControl Agent
    └── InsightReport Agent
```

### 2.2 各 Agent 职责

| Agent | 职责 | 核心工具 | Skill |
|:---|:---|:---|:---|
| **Orchestrator** | 意图识别、任务拆解、路由分发、状态追踪、冲突仲裁 | 任务管理、团队通信 | data-platform-agent-design |
| **DataSteward** | 数据治理、ETL 开发、数据质量监控、元数据管理 | database-mcp, spark-mcp | spark-data-processing |
| **TagProfiling** | 标签挖掘、特征工程、标签血缘、效果验证 | feast-mcp, spark-mcp | mlflow-model-management |
| **ModelFactory** | AutoML、特征筛选、模型训练、效果评估 | mlflow-mcp, spark-mcp | mlflow-model-management |
| **DMPAd** | 人群圈选、Lookalike、归因分析、策略模拟 | database-mcp, milvus-mcp | neo4j-graph-analytics |
| **RiskControl** | 图风控、反欺诈、信用评分、实时预警 | neo4j-mcp, spark-mcp | neo4j-graph-analytics |
| **InsightReport** | NL2SQL、归因分析、报告生成、可视化 | database-mcp, metabase-mcp | nl2sql-pattern |

### 2.3 Agent 消息协议

Agent 之间通过标准消息格式通信：

```json
{
  "type": "task_request",
  "from": "orchestrator",
  "to": "tag-profiling",
  "task_id": "task-001",
  "action": "create_tag",
  "payload": {
    "tag_name": "high_value_user",
    "dimensions": ["purchase_amount", "frequency", "recency"],
    "threshold": {
      "purchase_amount": ">1000",
      "frequency": ">=5",
      "recency": "<30days"
    }
  },
  "callback": "insight-report"
}
```

---

## 3. MCP Server 设计

### 3.1 MCP Server 列表

| Server | 协议 | 功能 | 外部依赖 |
|:---|:---|:---|:---|
| **database-mcp** | PostgreSQL/MySQL Wire | SQL 查询、ETL 任务下发、数据写入 | MySQL/PostgreSQL |
| **spark-mcp** | Livy REST API | Spark Job 提交、状态查询、日志获取 | Spark Cluster |
| **mlflow-mcp** | MLflow REST API | 模型注册、实验查询、模型部署 | MLflow Server |
| **neo4j-mcp** | Bolt Protocol | 图查询、团伙识别、路径分析 | Neo4j |
| **feast-mcp** | Feast gRPC | 特征读取、特征写入、特征血缘 | Feast Server |
| **milvus-mcp** | Milvus SDK | 向量检索、embedding 管理 | Milvus |
| **metabase-mcp** | Metabase API | 查询执行、可视化获取 | Metabase |
| **camunda-mcp** | Camunda REST API | 审批流触发、状态查询 | Camunda |

### 3.2 MCP Server 接口规范

#### 3.2.1 database-mcp

```python
# 工具定义
@mcp_server.tool()
def query(sql: str, max_rows: int = 1000) -> List[Dict]:
    """执行 SQL 查询"""

@mcp_server.tool()
def execute(sql: str) -> Dict:
    """执行 DDL/DML 语句"""

@mcp_server.tool()
def get_schema(table_name: str) -> Dict:
    """获取表结构信息"""
```

#### 3.2.2 spark-mcp

```python
@mcp_server.tool()
def submit_job(app_name: str, main_class: str, args: List[str]) -> str:
    """提交 Spark Job，返回 job_id"""

@mcp_server.tool()
def get_job_status(job_id: str) -> Dict:
    """查询 Job 状态"""

@mcp_server.tool()
def read_hive_table(db: str, table: str, limit: int = 1000) -> List[Dict]:
    """读取 Hive 表数据"""
```

#### 3.2.3 mlflow-mcp

```python
@mcp_server.tool()
def register_model(model_name: str, model_uri: str, metrics: Dict) -> str:
    """注册模型，返回 model_version"""

@mcp_server.tool()
def get_experiment(experiment_name: str) -> Dict:
    """获取实验详情"""

@mcp_server.tool()
def deploy_model(model_name: str, version: str, endpoint: str) -> Dict:
    """部署模型到推理端点"""
```

#### 3.2.4 neo4j-mcp

```python
@mcp_server.tool()
def query_graph(cypher: str, params: Dict) -> List[Dict]:
    """执行 Cypher 查询"""

@mcp_server.tool()
def find_fraud_pattern(entity_id: str, depth: int = 3) -> Dict:
    """基于实体查找欺诈团伙"""

@mcp_server.tool()
def get_credit_path(from_id: str, to_id: str) -> List[Dict]:
    """查询两个实体的关联路径"""
```

### 3.3 MCP Server 实现结构

```
mcp-servers/
├── requirements.txt
├── server.py                    # MCP Server 入口
├── config.py                     # 配置管理
├── services/
│   ├── database_service.py      # 数据库服务
│   ├── spark_service.py         # Spark 服务
│   ├── mlflow_service.py        # MLflow 服务
│   ├── neo4j_service.py         # Neo4j 服务
│   └── ...
└── README.md
```

---

## 4. Skill 设计

### 4.1 Skill 列表

| Skill | 用途 | 类型 |
|:---|:---|:---|
| **data-platform-agent-design** | 数据平台 Agent 架构设计模式 | Pattern |
| **spark-data-processing** | Spark Job 开发规范与最佳实践 | Technique |
| **mlflow-model-management** | MLflow 模型注册、部署、监控 | Technique |
| **neo4j-graph-analytics** | 图查询模式、反欺诈规则 | Pattern |
| **nl2sql-pattern** | NL2SQL 最佳实践、Prompt 模板 | Pattern |
| **hitl-approval-workflow** | 审批流配置规范 | Reference |

### 4.2 Skill 目录结构

```
skills/
├── data-platform-agent-design/
│   └── SKILL.md
├── spark-data-processing/
│   ├── SKILL.md
│   └── spark-job-template.py
├── mlflow-model-management/
│   ├── SKILL.md
│   └── model_registry_template.py
├── neo4j-graph-analytics/
│   ├── SKILL.md
│   └── cypher-templates/
│       ├── fraud_detection.cql
│       └── credit_path.cql
├── nl2sql-pattern/
│   ├── SKILL.md
│   └── prompt-templates/
│       ├── simple_query.prompt
│       └── complex_agg.prompt
└── hitl-approval-workflow/
    └── SKILL.md
```

---

## 5. 工作流设计

### 5.1 标签生命周期流程

```yaml
# workflows/tag-lifecycle.yaml
name: tag-lifecycle
description: 标签从创建到下线的全生命周期管理

states:
  - name: draft
    on_enter: create_tag_definition
  - name: validating
    on_enter: run_ab_test
    transitions:
      - to: approved
        condition: "accuracy >= 0.9 and coverage >= 0.85"
      - to: rejected
        condition: "accuracy < 0.9"
  - name: approved
    on_enter: register_tag
    transitions:
      - to: published
        condition: "hitl_approval == true"
  - name: published
    on_enter: enable_tag_api
    transitions:
      - to: monitoring
        after: "7d"
  - name: monitoring
    on_enter: enable_drift_check
    transitions:
      - to: degraded
        condition: "psi > 0.2"
      - to: archived
        condition: "usage < 100 and duration > 90d"
  - name: degraded
    on_enter: notify_owner
    transitions:
      - to: archived
        after: "30d"
  - name: archived
    final: true
```

### 5.2 风控评估流程

```yaml
# workflows/risk-assessment.yaml
name: risk-assessment
description: 贷前准入/贷中监控/贷后预警全流程

nodes:
  - name: credit_check
    agent: risk-control
    tools: [neo4j-mcp, spark-mcp]
    actions:
      - query_device_graph
      - calculate_transaction_velocity
      - run_credit_score_model
    output:
      risk_score: float
      risk_factors: list
      decision: approve/reject/review

  - name: fraud_detection
    agent: risk-control
    tools: [neo4j-mcp]
    actions:
      - find_fraud_pattern
      - detect_device_farm
      - check_ip_reputation
    output:
      fraud_probability: float
      fraud_tags: list

  - name: hitl_review
    condition: "risk_score > 0.8 or fraud_probability > 0.5"
    on_enter: create_approval_task
    tools: [camunda-mcp]

  - name: final_decision
    agent: orchestrator
    input: [credit_check, fraud_detection, hitl_review]
    output:
      final_decision: string
      explanation: string
```

### 5.3 广告优化流程

```yaml
# workflows/ad-optimization.yaml
name: ad-optimization
description: 广告投放从人群圈选到效果归因的全流程

stages:
  - name: audience_selection
    agent: dmp-ad
    tools: [database-mcp, milvus-mcp]
    actions:
      - define_target_segments
      - generate_lookalike
      - export_audience_package
    output: audience_package_id

  - name: strategy_simulation
    agent: dmp-ad
    actions:
      - load_historical_ctr_cvr
      - simulate_bid_strategy
      - calculate_expected_roi
    output: strategy_recommendations

  - name: campaign_execution
    condition: "strategy_approved"
    actions:
      - submit_to_dsp
      - configure_frequency_cap
      - set_budget_allocation

  - name: attribution_analysis
    agent: dmp-ad
    tools: [spark-mcp]
    schedule: "daily"
    actions:
      - calculate_multi_touch_attribution
      - analyze_conversion_path
      - generate_roi_report

  - name: optimization_feedback
    agent: model-factory
    actions:
      - update_ctr_prediction_model
      - retrain_lookalike_model
    trigger: "attribution_analysis complete"
```

---

## 6. 项目文件结构

```
p52_StarSight/
├── README.md                        # 项目说明
├── CLAUDE.md                        # Claude Code 配置
│
├── SKILL.md                         # 全局 skills 索引
│
├── skills/                          # Skills 目录
│   ├── data-platform-agent-design/
│   │   └── SKILL.md                # 架构设计 skill
│   ├── spark-data-processing/
│   │   ├── SKILL.md
│   │   └── spark-job-template.py
│   ├── mlflow-model-management/
│   │   ├── SKILL.md
│   │   └── model_registry_template.py
│   ├── neo4j-graph-analytics/
│   │   ├── SKILL.md
│   │   └── cypher-templates/
│   ├── nl2sql-pattern/
│   │   ├── SKILL.md
│   │   └── prompt-templates/
│   └── hitl-approval-workflow/
│       └── SKILL.md
│
├── mcp-servers/                    # MCP Servers 目录
│   ├── database-mcp/
│   │   ├── server.py
│   │   ├── config.py
│   │   ├── services/
│   │   └── requirements.txt
│   ├── spark-mcp/
│   ├── mlflow-mcp/
│   ├── neo4j-mcp/
│   ├── feast-mcp/
│   ├── milvus-mcp/
│   ├── metabase-mcp/
│   └── camunda-mcp/
│
├── agents/                         # Agents 目录
│   ├── orchestrator/
│   │   ├── agent.py
│   │   └── system.prompt
│   ├── data-steward/
│   │   ├── agent.py
│   │   └── system.prompt
│   ├── tag-profiling/
│   │   ├── agent.py
│   │   └── system.prompt
│   ├── model-factory/
│   │   ├── agent.py
│   │   └── system.prompt
│   ├── dmp-ad/
│   │   ├── agent.py
│   │   └── system.prompt
│   ├── risk-control/
│   │   ├── agent.py
│   │   └── system.prompt
│   └── insight-report/
│       ├── agent.py
│       └── system.prompt
│
├── workflows/                     # 工作流配置
│   ├── tag-lifecycle.yaml
│   ├── risk-assessment.yaml
│   ├── ad-optimization.yaml
│   └── nl2sql-query.yaml
│
├── docs/                          # 文档目录
│   ├── 项目需求文档PRD.md
│   ├── 需求分析文档.md
│   ├── Claude-Code-架构设计.md     # 本文档
│   └── API-Spec.md
│
├── scripts/                       # 脚本目录
│   ├── setup-mcp.sh
│   ├── setup-team.sh
│   └── run-workflow.sh
│
└── config/                       # 配置文件
    ├── mcp-servers.yaml
    ├── agents.yaml
    └── workflows.yaml
```

---

## 7. 实施路线图

### Phase 1: 基础搭建（M1-M2）

| 周次 | 任务 | 交付物 |
|:---|:---|:---|
| W1-2 | 搭建 MCP Server 基础框架，配置 database-mcp | MCP Server 骨架代码 |
| W3-4 | 实现 Orchestrator Agent，配置 TeamCreate | 团队结构定义 |
| W5-6 | 实现 DataSteward Agent，接入 spark-mcp | ETL 流水线 |
| W7-8 | 部署 data-platform-agent-design skill | 架构设计 Skill |

### Phase 2: 核心场景（M3-M4）

| 周次 | 任务 | 交付物 |
|:---|:---|:---|
| W9-10 | 实现 TagProfiling Agent + feast-mcp | 标签挖掘流程 |
| W11-12 | 实现 ModelFactory Agent + mlflow-mcp | AutoML 流水线 |
| W13-14 | 实现 DMPAd Agent + milvus-mcp | 人群圈选 + Lookalike |
| W15-16 | 实现 RiskControl Agent + neo4j-mcp | 风控模型 |

### Phase 3: 全链闭环（M5-M6）

| 周次 | 任务 | 交付物 |
|:---|:---|:---|
| W17-18 | 实现 InsightReport Agent + metabase-mcp | NL2SQL + 报告生成 |
| W19-20 | 配置 HITL 审批流 + camunda-mcp | 审批工作流 |
| W21-22 | 配置漂移监控 + 成本控制 | 运维监控体系 |
| W23-24 | 端到端集成测试 + 文档 | 完整系统交付 |

---

## 8. 参考文档

| 文档 | 路径 |
|:---|:---|
| 项目需求文档 | `docs/项目需求文档PRD.md` |
| 需求分析文档 | `docs/需求分析文档.md` |
| AI Agent 架构 Skill | `skills/data-platform-agent-design/SKILL.md` |

---

*文档结束*
