# Claude Code Configuration

## 项目概述

数据服务平台 - 基于 Claude Code Skills + Agents + MCP + Plugins 架构的企业级数据分析和商业化平台。

**项目状态**: MVP v1.0 - 基础骨架搭建完成

## 架构

```
Skills Layer → Agents Layer → MCP Servers Layer → External Systems
```

- **Skills**: `skills/` - 领域知识与最佳实践封装
- **Agents**: `agents/` - 7 个专业智能体
- **MCP Servers**: `mcp-servers/` - 外部系统对接
- **Workflows**: `config/workflows.yaml` - 业务流程定义

## 已实现 Skills

| Skill | 描述 |
|:---|:---|
| `data-platform-agent-design` | AI Agent 架构设计模式（5维需求分析法） |
| `spark-data-processing` | Spark Job 开发规范与模板 |

## 已实现 Agents

| Agent | 职责 |
|:---|:---|
| `orchestrator` | 意图识别、任务拆解、路由分发 |

## 已实现 MCP Servers

| Server | 功能 |
|:---|:---|
| `database-mcp` | SQL 查询、表结构、PostgreSQL/MySQL |
| `spark-mcp` | Livy Session、Batch Job、Hive 读取 |

## 关键文件

| 路径 | 描述 |
|:---|:---|
| `config/mcp-servers.yaml` | MCP Server 配置 |
| `config/agents.yaml` | Agent 配置 |
| `config/workflows.yaml` | 工作流配置 |
| `scripts/setup-team.py` | 项目验证脚本 |
| `docs/Claude-Code-架构设计.md` | 技术架构文档 |
| `docs/项目需求文档PRD.md` | 产品需求文档 |

## 验证命令

```bash
# 验证项目结构
python scripts/setup-team.py

# 启动 Database MCP Server
cd mcp-servers/database-mcp && python server.py

# 启动 Spark MCP Server
cd mcp-servers/spark-mcp && python server.py
```

## 开发命令

```bash
# 安装依赖
pip install -r requirements.txt

# 配置环境变量
export DB_HOST=localhost
export DB_PORT=5432
export DB_NAME=data_platform
export DB_USER=postgres
export DB_PASSWORD=your_password
export SPARK_LIVY_HOST=localhost
export SPARK_LIVY_PORT=8998
```
