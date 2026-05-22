"""
Spark MCP Server Configuration
"""

import os
from typing import Optional

import yaml
from pydantic import BaseModel


class SparkConfig(BaseModel):
    """Spark 配置模型"""
    name: str = "spark-mcp"
    protocol: str = "livy"
    host: str = "localhost"
    port: int = 8998
    spark_version: str = "3.4.0"
    deploy_mode: str = "cluster"
    queue: str = "default"
    timeout: int = 300

    @classmethod
    def from_yaml(cls, config_path: str, env_prefix: str = "SPARK_") -> "SparkConfig":
        """从 YAML 文件加载配置"""
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Config file not found: {config_path}")

        with open(config_path, 'r') as f:
            data = yaml.safe_load(f)

        spark_config = data.get('mcp_servers', {}).get('spark', {})

        def get_env(key: str, default: str = "") -> str:
            return os.getenv(f"{env_prefix}{key.upper()}", default)

        return cls(
            name=spark_config.get('name', 'spark-mcp'),
            protocol=spark_config.get('protocol', 'livy'),
            host=get_env('LIVY_HOST', spark_config.get('host', 'localhost')),
            port=int(get_env('LIVY_PORT', str(spark_config.get('port', 8998)))),
            spark_version=spark_config.get('spark_version', '3.4.0'),
            deploy_mode=get_env('DEPLOY_MODE', spark_config.get('deploy_mode', 'cluster')),
            queue=get_env('QUEUE', spark_config.get('queue', 'default')),
            timeout=int(get_env('TIMEOUT', str(spark_config.get('timeout', 300))))
        )


def load_config(config_path: Optional[str] = None) -> SparkConfig:
    """加载配置的便捷函数"""
    if config_path is None:
        project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
        config_path = os.path.join(project_root, "config", "mcp-servers.yaml")

    return SparkConfig.from_yaml(config_path)
