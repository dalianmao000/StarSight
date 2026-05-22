"""
Database MCP Server Configuration

配置文件，加载 config/mcp-servers.yaml 中的 database 配置。
"""

import os
from typing import Optional

import yaml
from pydantic import BaseModel


class DatabaseConfig(BaseModel):
    """数据库配置模型"""
    name: str = "database-mcp"
    protocol: str = "postgresql"
    host: str = "localhost"
    port: int = 5432
    database: str = ""
    user: str = ""
    password: str = ""
    pool_size: int = 10
    connection_timeout: int = 30

    @classmethod
    def from_yaml(cls, config_path: str, env_prefix: str = "DB_") -> "DatabaseConfig":
        """
        从 YAML 文件加载配置

        Args:
            config_path: YAML 配置文件路径
            env_prefix: 环境变量前缀，用于覆盖配置

        Returns:
            DatabaseConfig 实例
        """
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Config file not found: {config_path}")

        with open(config_path, 'r') as f:
            data = yaml.safe_load(f)

        # 从 yaml 中提取 database 配置
        db_config = data.get('mcp_servers', {}).get('database', {})

        # 环境变量覆盖
        def get_env(key: str, default: str = "") -> str:
            return os.getenv(f"{env_prefix}{key.upper()}", default)

        return cls(
            name=db_config.get('name', 'database-mcp'),
            protocol=get_env('PROTOCOL', db_config.get('protocol', 'postgresql')),
            host=get_env('HOST', db_config.get('host', 'localhost')),
            port=int(get_env('PORT', str(db_config.get('port', 5432)))),
            database=get_env('NAME', db_config.get('database', '')),
            user=get_env('USER', db_config.get('user', '')),
            password=get_env('PASSWORD', db_config.get('password', '')),
            pool_size=int(get_env('POOL_SIZE', str(db_config.get('pool_size', 10)))),
            connection_timeout=int(get_env('TIMEOUT', str(db_config.get('connection_timeout', 30))))
        )


def load_config(config_path: Optional[str] = None) -> DatabaseConfig:
    """
    加载配置的便捷函数

    Args:
        config_path: 配置文件路径，默认从项目根目录加载

    Returns:
        DatabaseConfig 实例
    """
    if config_path is None:
        # 默认路径
        project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
        config_path = os.path.join(project_root, "config", "mcp-servers.yaml")

    return DatabaseConfig.from_yaml(config_path)
