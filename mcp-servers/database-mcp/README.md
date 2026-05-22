# Database MCP Server

Database MCP Server 基于 Model Context Protocol (MCP) 提供数据库操作能力，支持 PostgreSQL 和 MySQL。

## 功能特性

- **SQL 查询**：执行 SELECT 查询，返回结构化结果
- **DDL/DML 执行**：执行建表、插入、更新等语句
- **元数据管理**：获取表结构、列出所有表
- **连接池**：基于 SQLAlchemy 的连接池管理
- **健康检查**：监控数据库连接状态

## 安装

```bash
pip install -r requirements.txt
```

## 配置

### 方式 1: 使用项目配置文件

项目根目录的 `config/mcp-servers.yaml` 中已配置：

```yaml
mcp_servers:
  database:
    protocol: postgresql
    host: ${DB_HOST}
    port: 5432
    database: ${DB_NAME}
    user: ${DB_USER}
    password: ${DB_PASSWORD}
```

### 方式 2: 环境变量

| 环境变量 | 说明 | 默认值 |
|:---|:---|:---|
| `DB_PROTOCOL` | 数据库协议 | postgresql |
| `DB_HOST` | 数据库地址 | localhost |
| `DB_PORT` | 端口 | 5432 |
| `DB_NAME` | 数据库名 | test |
| `DB_USER` | 用户名 | postgres |
| `DB_PASSWORD` | 密码 | - |
| `DB_POOL_SIZE` | 连接池大小 | 10 |

## 运行

### MCP Server 模式

```bash
python server.py
```

### 独立运行模式（测试）

```bash
python server.py
# 输出示例：
# === Database MCP Server - Standalone Mode ===
#
# 1. Health Check:
# {"success": true, "status": "healthy", "database": "test"}
```

## MCP Tools

| Tool | Description | Parameters |
|:---|:---|:---|
| `query` | 执行 SQL 查询 | `sql`, `max_rows`, `params` |
| `execute` | 执行 DDL/DML | `sql`, `params` |
| `get_schema` | 获取表结构 | `table_name`, `schema` |
| `list_tables` | 列出所有表 | `schema` |
| `health_check` | 健康检查 | - |

## 使用示例

### Python SDK 方式

```python
from server import DatabaseMCPServer

# 初始化
server = DatabaseMCPServer()
server.initialize()
service = server.service

# 查询
result = service.query("SELECT * FROM users LIMIT 10")
print(result)

# 获取表结构
result = service.get_schema("users")
print(result)
```

### MCP 协议方式

```json
{
  "jsonrpc": "2.0",
  "method": "tools/call",
  "params": {
    "name": "query",
    "arguments": {
      "sql": "SELECT * FROM users WHERE age > 18",
      "max_rows": 100
    }
  }
}
```

## 返回格式

### 成功响应

```json
{
  "success": true,
  "columns": ["id", "name", "age"],
  "rows": [
    {"id": 1, "name": "Alice", "age": 25},
    {"id": 2, "name": "Bob", "age": 30}
  ],
  "row_count": 2
}
```

### 错误响应

```json
{
  "success": false,
  "error": "relation 'users' does not exist",
  "error_type": "ProgrammingError"
}
```

## 目录结构

```
database-mcp/
├── server.py        # 主服务文件
├── config.py        # 配置加载
├── requirements.txt # 依赖
└── README.md        # 本文件
```

## License

Proprietary - Internal Use Only
