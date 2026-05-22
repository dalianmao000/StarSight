#!/usr/bin/env python3
"""
Spark MCP Server

基于 MCP (Model Context Protocol) 的 Spark 服务，
通过 Livy REST API 连接 Spark Cluster，提供 Job 提交和 Hive 读取功能。
"""

import os
import sys
import json
import logging
import time
from typing import Any, Dict, List, Optional
from datetime import datetime
from enum import Enum

import yaml
import httpx
from pydantic import BaseModel, Field

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
logger = logging.getLogger("spark-mcp")


# ========================================
# 配置模型
# ========================================

class SparkConfig(BaseModel):
    """Spark 配置"""
    name: str = "spark-mcp"
    protocol: str = "livy"
    host: str = "localhost"
    port: int = 8998
    spark_version: str = "3.4.0"
    deploy_mode: str = "cluster"
    queue: str = "default"
    timeout: int = 300


class JobStatus(str, Enum):
    """Job 状态枚举"""
    STARTING = "starting"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    DEAD = "dead"
    KILLED = "killed"


# ========================================
# Spark 服务
# ========================================

class SparkService:
    """Spark 服务类，通过 Livy API 连接 Spark Cluster"""

    def __init__(self, config: SparkConfig):
        self.config = config
        self.base_url = f"http://{config.host}:{config.port}"
        self.session_id: Optional[int] = None
        self._client: Optional[httpx.AsyncClient] = None

    @property
    def client(self) -> httpx.AsyncClient:
        """获取 HTTP 客户端"""
        if self._client is None:
            self._client = httpx.AsyncClient(
                base_url=self.base_url,
                timeout=httpx.Timeout(self.config.timeout)
            )
        return self._client

    async def close(self):
        """关闭客户端"""
        if self._client:
            await self._client.aclose()

    def _get_headers(self) -> Dict[str, str]:
        """获取请求头"""
        return {
            "Content-Type": "application/json",
            "Accept": "application/json"
        }

    # ========================================
    # Session 管理
    # ========================================

    async def create_session(self, kind: str = "spark") -> Dict[str, Any]:
        """
        创建 Livy Session

        Args:
            kind: session 类型 (spark / pyspark / sql / r)

        Returns:
            session 信息
        """
        payload = {
            "kind": kind,
            "conf": {
                "spark.master": f"yarn/{self.config.deploy_mode}",
                "spark.submit.deployMode": self.config.deploy_mode,
                "spark.yarn.queue": self.config.queue
            }
        }

        try:
            response = await self.client.post(
                "/sessions",
                json=payload,
                headers=self._get_headers()
            )
            response.raise_for_status()
            data = response.json()

            self.session_id = data["id"]
            logger.info(f"Created Livy session: {self.session_id}")

            return {
                "success": True,
                "session_id": self.session_id,
                "state": data.get("state")
            }
        except httpx.HTTPError as e:
            logger.error(f"Create session failed: {e}")
            return {
                "success": False,
                "error": str(e),
                "error_type": "HTTPError"
            }

    async def get_session(self, session_id: Optional[int] = None) -> Dict[str, Any]:
        """获取 Session 状态"""
        sid = session_id or self.session_id
        if not sid:
            return {"success": False, "error": "No session ID"}

        try:
            response = await self.client.get(
                f"/sessions/{sid}",
                headers=self._get_headers()
            )
            response.raise_for_status()
            return {
                "success": True,
                **response.json()
            }
        except httpx.HTTPError as e:
            return {"success": False, "error": str(e)}

    async def wait_for_session(self, session_id: Optional[int] = None, timeout: int = 60) -> bool:
        """等待 Session 就绪"""
        sid = session_id or self.session_id
        if not sid:
            return False

        start_time = time.time()
        while time.time() - start_time < timeout:
            result = await self.get_session(sid)
            if result.get("success"):
                state = result.get("state", "")
                if state == "idle":
                    return True
                elif state in ["shutting_down", "dead", "error"]:
                    return False
            await self._async_sleep(2)

        return False

    async def delete_session(self, session_id: Optional[int] = None) -> Dict[str, Any]:
        """删除 Session"""
        sid = session_id or self.session_id
        if not sid:
            return {"success": False, "error": "No session ID"}

        try:
            response = await self.client.delete(
                f"/sessions/{sid}",
                headers=self._get_headers()
            )
            response.raise_for_status()
            self.session_id = None
            return {"success": True}
        except httpx.HTTPError as e:
            return {"success": False, "error": str(e)}

    # ========================================
    # Statement 执行
    # ========================================

    async def execute(self, code: str, session_id: Optional[int] = None) -> Dict[str, Any]:
        """
        执行代码

        Args:
            code: 要执行的代码
            session_id: session ID

        Returns:
            执行结果
        """
        sid = session_id or self.session_id
        if not sid:
            # 自动创建 session
            create_result = await self.create_session()
            if not create_result.get("success"):
                return create_result
            await self.wait_for_session()

        try:
            response = await self.client.post(
                f"/sessions/{self.session_id}/statements",
                json={"code": code},
                headers=self._get_headers()
            )
            response.raise_for_status()
            data = response.json()

            return {
                "success": True,
                "statement_id": data["id"],
                "state": data.get("state")
            }
        except httpx.HTTPError as e:
            return {"success": False, "error": str(e)}

    async def get_statement_result(self, statement_id: int, session_id: Optional[int] = None) -> Dict[str, Any]:
        """获取 Statement 结果"""
        sid = session_id or self.session_id
        if not sid:
            return {"success": False, "error": "No session ID"}

        try:
            response = await self.client.get(
                f"/sessions/{sid}/statements/{statement_id}",
                headers=self._get_headers()
            )
            response.raise_for_status()
            data = response.json()

            # 解析输出
            output = data.get("output", {})
            result_type = output.get("status", "")

            if result_type == "ok":
                return {
                    "success": True,
                    "state": data.get("state"),
                    "result": output.get("data"),
                    "type": output.get("type")
                }
            else:
                return {
                    "success": False,
                    "state": data.get("state"),
                    "error": output.get("ename"),
                    "traceback": output.get("traceback")
                }
        except httpx.HTTPError as e:
            return {"success": False, "error": str(e)}

    async def execute_and_wait(self, code: str, session_id: Optional[int] = None, timeout: int = 120) -> Dict[str, Any]:
        """执行代码并等待结果"""
        # 先执行
        exec_result = await self.execute(code, session_id)
        if not exec_result.get("success"):
            return exec_result

        statement_id = exec_result.get("statement_id")
        start_time = time.time()

        # 轮询等待结果
        while time.time() - start_time < timeout:
            result = await self.get_statement_result(statement_id, session_id)
            state = result.get("state", "")

            if state == "available":
                return result
            elif state in ["error", "cancelled"]:
                return result

            await self._async_sleep(2)

        return {
            "success": False,
            "error": "Timeout waiting for result"
        }

    # ========================================
    # Batch Job
    # ========================================

    async def submit_batch(self, file: str, args: Optional[List[str]] = None,
                          conf: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        """
        提交 Batch Job

        Args:
            file: 要执行的 Python 文件路径 (hdfs:// 或 local://)
            args: 命令行参数
            conf: Spark 配置

        Returns:
            batch ID
        """
        payload = {
            "file": file,
            "args": args or [],
            "conf": conf or {
                "spark.yarn.queue": self.config.queue
            }
        }

        try:
            response = await self.client.post(
                "/batches",
                json=payload,
                headers=self._get_headers()
            )
            response.raise_for_status()
            data = response.json()

            return {
                "success": True,
                "batch_id": data["id"],
                "state": data.get("state")
            }
        except httpx.HTTPError as e:
            return {"success": False, "error": str(e)}

    async def get_batch(self, batch_id: int) -> Dict[str, Any]:
        """获取 Batch Job 状态"""
        try:
            response = await self.client.get(
                f"/batches/{batch_id}",
                headers=self._get_headers()
            )
            response.raise_for_status()
            return {
                "success": True,
                **response.json()
            }
        except httpx.HTTPError as e:
            return {"success": False, "error": str(e)}

    async def kill_batch(self, batch_id: int) -> Dict[str, Any]:
        """终止 Batch Job"""
        try:
            response = await self.client.delete(
                f"/batches/{batch_id}",
                headers=self._get_headers()
            )
            response.raise_for_status()
            return {"success": True}
        except httpx.HTTPError as e:
            return {"success": False, "error": str(e)}

    async def wait_for_batch(self, batch_id: int, timeout: int = 600) -> Dict[str, Any]:
        """等待 Batch Job 完成"""
        start_time = time.time()

        while time.time() - start_time < timeout:
            result = await self.get_batch(batch_id)
            if not result.get("success"):
                return result

            state = result.get("state", "")
            if state == "success":
                return result
            elif state in ["failed", "killed", "dead"]:
                return {
                    "success": False,
                    "error": f"Batch {batch_id} ended with state: {state}",
                    "state": state
                }

            await self._async_sleep(10)

        return {
            "success": False,
            "error": "Timeout waiting for batch"
        }

    # ========================================
    # Hive 操作
    # ========================================

    async def read_hive_table(self, db: str, table: str, limit: int = 1000) -> Dict[str, Any]:
        """
        读取 Hive 表数据

        Args:
            db: 数据库名
            table: 表名
            limit: 限制行数

        Returns:
            表数据
        """
        code = f"""
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
df = spark.sql("SELECT * FROM {db}.{table} LIMIT {limit}")
df.collect()
"""
        return await self.execute_and_wait(code)

    async def show_tables(self, db: str) -> Dict[str, Any]:
        """列出数据库中的表"""
        code = f"""
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
spark.sql("SHOW TABLES IN {db}").show()
"""
        return await self.execute_and_wait(code)

    async def show_databases(self) -> Dict[str, Any]:
        """列出所有数据库"""
        code = """
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
spark.sql("SHOW DATABASES").show()
"""
        return await self.execute_and_wait(code)

    async def describe_table(self, db: str, table: str) -> Dict[str, Any]:
        """获取表结构"""
        code = f"""
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
spark.sql("DESCRIBE TABLE {db}.{table}").show()
"""
        return await self.execute_and_wait(code)

    # ========================================
    # 健康检查
    # ========================================

    async def health_check(self) -> Dict[str, Any]:
        """健康检查"""
        try:
            response = await self.client.get("/")
            response.raise_for_status()
            data = response.json()

            return {
                "success": True,
                "status": "healthy",
                "version": data.get("version"),
                "server": data.get("server")
            }
        except httpx.HTTPError as e:
            return {
                "success": False,
                "status": "unhealthy",
                "error": str(e)
            }

    # ========================================
    # 辅助方法
    # ========================================

    @staticmethod
    async def _async_sleep(seconds: float):
        """异步睡眠"""
        import asyncio
        await asyncio.sleep(seconds)


# ========================================
# MCP Server 实现
# ========================================

class SparkMCPServer:
    """Spark MCP Server"""

    def __init__(self, config_path: str = None):
        self.config_path = config_path
        self.config: Optional[SparkConfig] = None
        self.service: Optional[SparkService] = None

    def load_config(self, config_path: str = None) -> SparkConfig:
        """加载配置"""
        config_path = config_path or self.config_path

        if config_path and os.path.exists(config_path):
            with open(config_path, 'r') as f:
                data = yaml.safe_load(f)
                spark_config = data.get('mcp_servers', {}).get('spark', {})
                self.config = SparkConfig(**spark_config)
        else:
            self.config = SparkConfig(
                host=os.getenv("SPARK_LIVY_HOST", "localhost"),
                port=int(os.getenv("SPARK_LIVY_PORT", "8998")),
                deploy_mode=os.getenv("SPARK_DEPLOY_MODE", "cluster"),
                queue=os.getenv("SPARK_QUEUE", "default")
            )

        return self.config

    async def initialize(self):
        """初始化服务"""
        config = self.load_config(self.config_path)
        self.service = SparkService(config)
        logger.info("Spark MCP Server initialized")

    def get_tools(self) -> List[Tool]:
        """获取 MCP 工具列表"""
        if not MCP_AVAILABLE:
            return []

        return [
            Tool(
                name="create_session",
                description="创建 Livy Spark Session",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "kind": {"type": "string", "enum": ["spark", "pyspark", "sql"], "default": "pyspark"}
                    }
                }
            ),
            Tool(
                name="execute",
                description="执行 Spark 代码",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "code": {"type": "string", "description": "要执行的 Python/Spark 代码"},
                        "session_id": {"type": "integer", "description": "Session ID"}
                    },
                    "required": ["code"]
                }
            ),
            Tool(
                name="submit_batch",
                description="提交 Spark Batch Job",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "file": {"type": "string", "description": "Python 文件路径 (hdfs:// 或 local://)"},
                        "args": {"type": "array", "description": "命令行参数"},
                        "conf": {"type": "object", "description": "Spark 配置"}
                    },
                    "required": ["file"]
                }
            ),
            Tool(
                name="read_hive_table",
                description="读取 Hive 表数据",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "db": {"type": "string", "description": "数据库名"},
                        "table": {"type": "string", "description": "表名"},
                        "limit": {"type": "integer", "description": "限制行数", "default": 1000}
                    },
                    "required": ["db", "table"]
                }
            ),
            Tool(
                name="show_tables",
                description="列出数据库中的表",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "db": {"type": "string", "description": "数据库名"}
                    },
                    "required": ["db"]
                }
            ),
            Tool(
                name="describe_table",
                description="获取表结构",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "db": {"type": "string", "description": "数据库名"},
                        "table": {"type": "string", "description": "表名"}
                    },
                    "required": ["db", "table"]
                }
            ),
            Tool(
                name="get_session",
                description="获取 Session 状态",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "session_id": {"type": "integer", "description": "Session ID"}
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

    async def handle_tool_call(self, tool_name: str, arguments: Dict) -> TextContent:
        """处理工具调用"""
        if not self.service:
            await self.initialize()

        if tool_name == "create_session":
            result = await self.service.create_session(arguments.get("kind", "pyspark"))
        elif tool_name == "execute":
            result = await self.service.execute_and_wait(
                code=arguments["code"],
                session_id=arguments.get("session_id")
            )
        elif tool_name == "submit_batch":
            result = await self.service.submit_batch(
                file=arguments["file"],
                args=arguments.get("args"),
                conf=arguments.get("conf")
            )
        elif tool_name == "read_hive_table":
            result = await self.service.read_hive_table(
                db=arguments["db"],
                table=arguments["table"],
                limit=arguments.get("limit", 1000)
            )
        elif tool_name == "show_tables":
            result = await self.service.show_tables(arguments["db"])
        elif tool_name == "describe_table":
            result = await self.service.describe_table(
                db=arguments["db"],
                table=arguments["table"]
            )
        elif tool_name == "get_session":
            result = await self.service.get_session(arguments.get("session_id"))
        elif tool_name == "health_check":
            result = await self.service.health_check()
        else:
            result = {"success": False, "error": f"Unknown tool: {tool_name}"}

        return TextContent(type="text", text=json.dumps(result, ensure_ascii=False, indent=2))


# ========================================
# 独立运行模式
# ========================================

async def run_standalone():
    """独立运行模式"""
    server = SparkMCPServer()

    # 加载配置
    config_path = os.path.join(os.path.dirname(__file__), "../../../config/mcp-servers.yaml")
    if os.path.exists(config_path):
        server.load_config(config_path)
    else:
        server.config = SparkConfig()

    try:
        await server.initialize()
        service = server.service

        print("\n=== Spark MCP Server - Standalone Mode ===\n")

        # 健康检查
        print("1. Health Check:")
        result = await service.health_check()
        print(json.dumps(result, indent=2))

        # 创建 Session
        print("\n2. Create Session:")
        result = await service.create_session("pyspark")
        print(json.dumps(result, indent=2))

        if result.get("success"):
            session_id = result.get("session_id")
            print(f"\n3. Waiting for session {session_id}...")
            ready = await service.wait_for_session(session_id)
            print(f"Session ready: {ready}")

            # 执行简单查询
            if ready:
                print("\n4. Execute Code:")
                code = "print('Hello from Spark!')"
                result = await service.execute_and_wait(code, session_id)
                print(json.dumps(result, indent=2))

            # 清理
            print("\n5. Delete Session:")
            result = await service.delete_session(session_id)
            print(json.dumps(result, indent=2))

    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await server.service.close()


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

    server = SparkMCPServer()
    await server.initialize()

    mcp_server = Server("spark-mcp")

    @mcp_server.list_tools()
    async def list_tools():
        return server.get_tools()

    @mcp_server.call_tool()
    async def call_tool(name: str, arguments: Dict):
        result = await server.handle_tool_call(name, arguments)
        return [result]

    async with stdio_server() as (read_stream, write_stream):
        await mcp_server.run(read_stream, write_stream, server.initialize)


# ========================================
# 入口
# ========================================

if __name__ == "__main__":
    if MCP_AVAILABLE:
        import asyncio
        asyncio.run(run_mcp_server())
    else:
        print("MCP package not available, running in standalone mode")
        import asyncio
        asyncio.run(run_standalone())