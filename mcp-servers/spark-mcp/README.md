# Spark MCP Server

基于 Model Context Protocol (MCP) 的 Spark 服务，通过 Livy REST API 连接 Spark Cluster。

## 功能特性

- **Session 管理**：创建、查询、删除 Livy Session
- **代码执行**：交互式执行 PySpark / SparkSQL 代码
- **Batch Job 提交**：提交独立的 Spark Job 到集群
- **Hive 表读取**：直接查询 Hive 表数据
- **元数据查询**：列出数据库、表，查看表结构

## 安装

```bash
pip install -r requirements.txt
```

## 配置

### 环境变量

| 环境变量 | 说明 | 默认值 |
|:---|:---|:---|
| `SPARK_LIVY_HOST` | Livy Server 地址 | localhost |
| `SPARK_LIVY_PORT` | Livy Server 端口 | 8998 |
| `SPARK_DEPLOY_MODE` | Spark 部署模式 | cluster |
| `SPARK_QUEUE` | YARN 队列 | default |

## 运行

```bash
python server.py
```

## MCP Tools

| Tool | Description | Parameters |
|:---|:---|:---|
| `create_session` | 创建 Livy Session | `kind` (spark/pyspark/sql) |
| `execute` | 执行 Spark 代码 | `code`, `session_id` |
| `submit_batch` | 提交 Batch Job | `file`, `args`, `conf` |
| `read_hive_table` | 读取 Hive 表 | `db`, `table`, `limit` |
| `show_tables` | 列出表 | `db` |
| `describe_table` | 查看表结构 | `db`, `table` |
| `get_session` | 获取 Session 状态 | `session_id` |
| `health_check` | 健康检查 | - |

## 使用示例

### 读取 Hive 表

```python
result = await service.read_hive_table("default", "users", limit=100)
```

### 执行 PySpark 代码

```python
code = """
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
df = spark.sql("SELECT count(*) FROM default.users")
df.show()
"""
result = await service.execute_and_wait(code)
```

### 提交 Batch Job

```python
result = await service.submit_batch(
    file="hdfs:///path/to/app.py",
    args=["--input", "hdfs:///data/input"],
    conf={"spark.executor.memory": "4g"}
)
```

## 目录结构

```
spark-mcp/
├── server.py        # 主服务文件
├── config.py       # 配置加载
├── requirements.txt # 依赖
└── README.md        # 本文件
```

## License

Proprietary - Internal Use Only
