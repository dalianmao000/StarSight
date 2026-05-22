#!/usr/bin/env python3
"""
Database MCP Server

基于 MCP (Model Context Protocol) 的数据库服务，
支持 PostgreSQL 和 MySQL，提供 SQL 查询、元数据管理等功能。
"""

import os
import sys
import json
import logging
from typing import Any, Dict, List, Optional
from contextlib import asynccontextmanager

import yaml
from pydantic import BaseModel, Field
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.pool import QueuePool
from sqlalchemy.exc import SQLAlchemyError

# MCP Server 框架
try:
    from mcp.server import MCPServer
    from mcp.types import Tool, TextContent
    MCP_AVAILABLE = True
except ImportError:
    MCP_AVAILABLE = False
    print("Warning: mcp package not available, running in standalone mode")

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("database-mcp")


# ========================================
# 配置模型
# ========================================

class DatabaseConfig(BaseModel):
    """数据库配置"""
    protocol: str = "postgresql"
    host: str = "localhost"
    port: int = 5432
    database: str = ""
    user: str = ""
    password: str = ""
    pool_size: int = 10
    connection_timeout: int = 30


class QueryRequest(BaseModel):
    """查询请求模型"""
    sql: str = Field(..., description="要执行的 SQL 语句")
    max_rows: int = Field(default=1000, description="最大返回行数")
    params: Optional[Dict[str, Any]] = Field(default=None, description="查询参数")


class ExecuteRequest(BaseModel):
    """执行请求模型"""
    sql: str = Field(..., description="要执行的 DDL/DML 语句")
    params: Optional[Dict[str, Any]] = Field(default=None, description="执行参数")


# ========================================
# 数据库服务
# ========================================

class DatabaseService:
    """数据库服务类"""

    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.engine = None
        self._create_engine()

    def _create_engine(self):
        """创建 SQLAlchemy 引擎"""
        # 构建连接 URL
        if self.config.protocol == "postgresql":
            url = f"postgresql://{self.config.user}:{self.config.password}@{self.config.host}:{self.config.port}/{self.config.database}"
        elif self.config.protocol == "mysql":
            url = f"mysql+pymysql://{self.config.user}:{self.config.password}@{self.config.host}:{self.config.port}/{self.config.database}"
        else:
            raise ValueError(f"Unsupported protocol: {self.config.protocol}")

        # 创建引擎 with 连接池
        self.engine = create_engine(
            url,
            poolclass=QueuePool,
            pool_size=self.config.pool_size,
            max_overflow=10,
            pool_timeout=self.config.connection_timeout,
            pool_pre_ping=True,  # 连接前检查
            echo=False  # 生产环境设为 False
        )
        logger.info(f"Database engine created: {self.config.protocol}://{self.config.host}:{self.config.port}/{self.config.database}")

    def query(self, sql: str, max_rows: int = 1000, params: Optional[Dict] = None) -> Dict[str, Any]:
        """执行查询 SQL"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(sql), params or {})
                rows = result.fetchmany(max_rows)

                # 获取列名
                columns = list(result.keys())

                return {
                    "success": True,
                    "columns": columns,
                    "rows": [dict(zip(columns, row)) for row in rows],
                    "row_count": len(rows),
                    "execution_time": 0  # 简化版本，可添加计时
                }
        except SQLAlchemyError as e:
            logger.error(f"Query failed: {str(e)}")
            return {
                "success": False,
                "error": str(e),
                "error_type": type(e).__name__
            }

    def execute(self, sql: str, params: Optional[Dict] = None) -> Dict[str, Any]:
        """执行 DDL/DML 语句"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(sql), params or {})
                conn.commit()

                return {
                    "success": True,
                    "row_count": result.rowcount if result.rowcount >= 0 else 0,
                    "last_insert_id": result.last_inserted_id() if hasattr(result, 'last_inserted_id') else None
                }
        except SQLAlchemyError as e:
            logger.error(f"Execute failed: {str(e)}")
            return {
                "success": False,
                "error": str(e),
                "error_type": type(e).__name__
            }

    def get_schema(self, table_name: str, schema: Optional[str] = None) -> Dict[str, Any]:
        """获取表结构信息"""
        try:
            inspector = inspect(self.engine)

            # 获取列信息
            columns = inspector.get_columns(table_name, schema=schema)

            # 获取主键
            primary_keys = inspector.get_pk_constraint(table_name, schema=schema)
            pk_columns = primary_keys.get('constrained_columns', [])

            # 获取外键
            foreign_keys = inspector.get_foreign_keys(table_name, schema=schema)

            # 获取索引
            indexes = inspector.get_indexes(table_name, schema=schema)

            return {
                "success": True,
                "table_name": table_name,
                "schema": schema,
                "columns": [
                    {
                        "name": col["name"],
                        "type": str(col["type"]),
                        "nullable": col["nullable"],
                        "default": col.get("default"),
                        "is_primary_key": col["name"] in pk_columns
                    }
                    for col in columns
                ],
                "primary_keys": pk_columns,
                "foreign_keys": foreign_keys,
                "indexes": indexes
            }
        except SQLAlchemyError as e:
            logger.error(f"Get schema failed: {str(e)}")
            return {
                "success": False,
                "error": str(e),
                "error_type": type(e).__name__
            }

    def list_tables(self, schema: Optional[str] = None) -> Dict[str, Any]:
        """列出所有表"""
        try:
            inspector = inspect(self.engine)
            tables = inspector.get_table_names(schema=schema)

            return {
                "success": True,
                "tables": tables,
                "schema": schema
            }
        except SQLAlchemyError as e:
            logger.error(f"List tables failed: {str(e)}")
            return {
                "success": False,
                "error": str(e),
                "error_type": type(e).__name__
            }

    def health_check(self) -> Dict[str, Any]:
        """健康检查"""
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return {
                "success": True,
                "status": "healthy",
                "database": self.config.database
            }
        except SQLAlchemyError as e:
            return {
                "success": False,
                "status": "unhealthy",
                "error": str(e)
            }

    def close(self):
        """关闭引擎"""
        if self.engine:
            self.engine.dispose()
            logger.info("Database engine disposed")


# ========================================
# MCP Server 实现
# ========================================

class DatabaseMCPServer:
    """Database MCP Server"""

    def __init__(self, config_path: str = None):
        self.config_path = config_path
        self.config: Optional[DatabaseConfig] = None
        self.service: Optional[DatabaseService] = None

    def load_config(self, config_path: str = None) -> DatabaseConfig:
        """加载配置"""
        config_path = config_path or self.config_path

        if config_path and os.path.exists(config_path):
            with open(config_path, 'r') as f:
                data = yaml.safe_load(f)
                # 从 yaml 中提取 database 配置
                db_config = data.get('mcp_servers', {}).get('database', {})
                self.config = DatabaseConfig(**db_config)
        else:
            # 从环境变量构建配置
            protocol = os.getenv("DB_PROTOCOL", "postgresql")
            self.config = DatabaseConfig(
                protocol=protocol,
                host=os.getenv("DB_HOST", "localhost"),
                port=int(os.getenv("DB_PORT", "5432")),
                database=os.getenv("DB_NAME", "test"),
                user=os.getenv("DB_USER", "postgres"),
                password=os.getenv("DB_PASSWORD", ""),
                pool_size=int(os.getenv("DB_POOL_SIZE", "10"))
            )

        return self.config

    def initialize(self):
        """初始化服务"""
        config = self.load_config(self.config_path)
        self.service = DatabaseService(config)
        logger.info("Database MCP Server initialized")

    # ========================================
    # MCP Tools (当 MCP 框架可用时)
    # ========================================

    def get_tools(self) -> List[Tool]:
        """获取 MCP 工具列表"""
        if not MCP_AVAILABLE:
            return []

        return [
            Tool(
                name="query",
                description="执行 SQL 查询语句",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "sql": {"type": "string", "description": "SQL 查询语句"},
                        "max_rows": {"type": "integer", "description": "最大返回行数", "default": 1000},
                        "params": {"type": "object", "description": "查询参数"}
                    },
                    "required": ["sql"]
                }
            ),
            Tool(
                name="execute",
                description="执行 DDL/DML 语句",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "sql": {"type": "string", "description": "DDL/DML 语句"},
                        "params": {"type": "object", "description": "执行参数"}
                    },
                    "required": ["sql"]
                }
            ),
            Tool(
                name="get_schema",
                description="获取表结构信息",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "table_name": {"type": "string", "description": "表名"},
                        "schema": {"type": "string", "description": "Schema 名"}
                    },
                    "required": ["table_name"]
                }
            ),
            Tool(
                name="list_tables",
                description="列出所有表",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "schema": {"type": "string", "description": "Schema 名"}
                    }
                }
            ),
            Tool(
                name="health_check",
                description="健康检查",
                inputSchema={
                    "type": "object",
                    "properties": {}
                }
            )
        ]

    def handle_tool_call(self, tool_name: str, arguments: Dict) -> TextContent:
        """处理工具调用"""
        if not self.service:
            self.initialize()

        if tool_name == "query":
            result = self.service.query(
                sql=arguments["sql"],
                max_rows=arguments.get("max_rows", 1000),
                params=arguments.get("params")
            )
        elif tool_name == "execute":
            result = self.service.execute(
                sql=arguments["sql"],
                params=arguments.get("params")
            )
        elif tool_name == "get_schema":
            result = self.service.get_schema(
                table_name=arguments["table_name"],
                schema=arguments.get("schema")
            )
        elif tool_name == "list_tables":
            result = self.service.list_tables(
                schema=arguments.get("schema")
            )
        elif tool_name == "health_check":
            result = self.service.health_check()
        else:
            result = {"success": False, "error": f"Unknown tool: {tool_name}"}

        return TextContent(type="text", text=json.dumps(result, ensure_ascii=False, indent=2))


# ========================================
# 独立运行模式
# ========================================

def run_standalone():
    """独立运行模式（无 MCP 框架）"""
    server = DatabaseMCPServer()

    # 尝试加载配置文件
    config_path = os.path.join(os.path.dirname(__file__), "../../../config/mcp-servers.yaml")
    if os.path.exists(config_path):
        server.load_config(config_path)
    else:
        # 使用默认配置
        server.config = DatabaseConfig(
            protocol="postgresql",
            host="localhost",
            port=5432,
            database="test",
            user="postgres",
            password="postgres"
        )

    try:
        server.initialize()
        service = server.service

        # 示例查询
        print("\n=== Database MCP Server - Standalone Mode ===\n")

        # 健康检查
        print("1. Health Check:")
        result = service.health_check()
        print(json.dumps(result, indent=2))

        # 列出表
        print("\n2. List Tables:")
        result = service.list_tables()
        print(json.dumps(result, indent=2))

        # 简单查询示例（PostgreSQL）
        print("\n3. Example Query (pg_catalog):")
        result = service.query("SELECT version()", max_rows=1)
        print(json.dumps(result, indent=2))

    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()


# ========================================
# MCP Server 运行模式
# ========================================

async def run_mcp_server():
    """MCP Server 运行模式"""
    if not MCP_AVAILABLE:
        print("Error: mcp package not available")
        return

    from mcp.server import Server
    from mcp.server.stdio import stdio_server
    import asyncio

    server = DatabaseMCPServer()
    server.initialize()

    mcp_server = Server("database-mcp")

    @mcp_server.list_tools()
    async def list_tools():
        return server.get_tools()

    @mcp_server.call_tool()
    async def call_tool(name: str, arguments: Dict):
        result = server.handle_tool_call(name, arguments)
        return [result]

    async with stdio_server() as (read_stream, write_stream):
        await mcp_server.run(read_stream, write_stream, server.initialize)


# ========================================
# 入口
# ========================================

if __name__ == "__main__":
    if MCP_AVAILABLE:
        # MCP 模式
        import asyncio
        asyncio.run(run_mcp_server())
    else:
        # 独立模式
        run_standalone()
