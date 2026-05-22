#!/usr/bin/env python3
"""
Team Setup and Test Script

验证数据平台项目结构和组件配置。
注意：MCP Servers 和 Agents 需要单独部署，这里只验证文件结构和配置。
"""

import os
import sys
import logging
from datetime import datetime
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("team-setup")


def check_file_exists(path: str) -> bool:
    """检查文件是否存在"""
    return os.path.exists(path)


def check_directory_structure():
    """检查目录结构"""
    logger.info("Checking directory structure...")

    base_dir = Path(__file__).parent.parent

    required_dirs = [
        "agents/orchestrator",
        "agents/data-steward",
        "agents/tag-profiling",
        "agents/model-factory",
        "agents/dmp-ad",
        "agents/risk-control",
        "agents/insight-report",
        "mcp-servers/database-mcp",
        "mcp-servers/spark-mcp",
        "mcp-servers/mlflow-mcp",
        "mcp-servers/neo4j-mcp",
        "mcp-servers/feast-mcp",
        "mcp-servers/milvus-mcp",
        "mcp-servers/metabase-mcp",
        "mcp-servers/camunda-mcp",
        "skills/data-platform-agent-design",
        "skills/spark-data-processing",
        "skills/mlflow-model-management",
        "skills/neo4j-graph-analytics",
        "skills/nl2sql-pattern",
        "skills/hitl-approval-workflow",
        "config",
        "workflows",
        "docs",
    ]

    results = []
    for dir_path in required_dirs:
        full_path = base_dir / dir_path
        exists = full_path.exists()
        status = "✓" if exists else "✗"
        results.append((dir_path, exists))
        logger.info(f"  {status} {dir_path}")

    passed = sum(1 for _, e in results if e)
    total = len(results)
    logger.info(f"\nDirectory check: {passed}/{total} directories exist")
    return passed == total


def check_config_files():
    """检查配置文件"""
    logger.info("\nChecking config files...")

    base_dir = Path(__file__).parent.parent
    config_dir = base_dir / "config"

    required_configs = [
        "mcp-servers.yaml",
        "agents.yaml",
        "workflows.yaml",
    ]

    results = []
    for config_file in required_configs:
        full_path = config_dir / config_file
        exists = full_path.exists()
        status = "✓" if exists else "✗"
        results.append((config_file, exists))
        logger.info(f"  {status} {config_file}")

    passed = sum(1 for _, e in results if e)
    total = len(results)
    logger.info(f"\nConfig check: {passed}/{total} config files exist")
    return passed == total


def check_skills():
    """检查 Skills"""
    logger.info("\nChecking Skills...")

    base_dir = Path(__file__).parent.parent
    skills_dir = base_dir / "skills"

    required_skills = [
        "data-platform-agent-design/SKILL.md",
        "spark-data-processing/SKILL.md",
    ]

    results = []
    for skill_path in required_skills:
        full_path = skills_dir / skill_path
        exists = full_path.exists()
        status = "✓" if exists else "✗"
        results.append((skill_path, exists))
        logger.info(f"  {status} {skill_path}")

    passed = sum(1 for _, e in results if e)
    total = len(results)
    logger.info(f"\nSkills check: {passed}/{total} skills exist")
    return passed == total


def check_mcp_servers():
    """检查 MCP Servers"""
    logger.info("\nChecking MCP Servers...")

    base_dir = Path(__file__).parent.parent
    mcp_dir = base_dir / "mcp-servers"

    required_servers = [
        "database-mcp/server.py",
        "spark-mcp/server.py",
    ]

    results = []
    for server_path in required_servers:
        full_path = mcp_dir / server_path
        exists = full_path.exists()
        status = "✓" if exists else "✗"
        results.append((server_path, exists))
        logger.info(f"  {status} {server_path}")

    passed = sum(1 for _, e in results if e)
    total = len(results)
    logger.info(f"\nMCP Servers check: {passed}/{total} servers exist")
    return passed == total


def check_agents():
    """检查 Agents"""
    logger.info("\nChecking Agents...")

    base_dir = Path(__file__).parent.parent
    agents_dir = base_dir / "agents"

    required_agents = [
        "orchestrator/agent.py",
        "orchestrator/system.prompt",
    ]

    results = []
    for agent_path in required_agents:
        full_path = agents_dir / agent_path
        exists = full_path.exists()
        status = "✓" if exists else "✗"
        results.append((agent_path, exists))
        logger.info(f"  {status} {agent_path}")

    passed = sum(1 for _, e in results if e)
    total = len(results)
    logger.info(f"\nAgents check: {passed}/{total} agents exist")
    return passed == total


def test_orchestrator_intent_recognition():
    """测试 Orchestrator Agent 意图识别（不需要导入模块）"""
    logger.info("\nTesting Orchestrator Intent Recognition...")

    # 意图关键词定义（与 agent.py 中一致）
    routing_rules = [
        (["金融", "风控", "信用", "欺诈", "风险", "贷款", "信贷"], "risk-control", "risk_control"),
        (["标签", "画像", "人群", "圈选", "用户分群", "高价值"], "tag-profiling", "tag_query"),
        (["训练", "模型", "预测", "机器学习", "AutoML", "特征"], "model-factory", "model_training"),
        (["广告", "投放", "DMP", "定向", "归因", "Lookalike", "CTR", "CVR", "ROI"], "dmp-ad", "ad_optimization"),
        (["分析", "洞察", "报告", "查询", "NL2SQL", "自然语言", "SQL"], "insight-report", "data_analysis"),
        (["数据质量", "ETL", "同步", "治理", "数据源"], "data-steward", "data_governance"),
    ]

    test_cases = [
        ("帮我分析Q3广告效果", "dmp-ad"),  # 广告优先于分析（规则顺序）
        ("查询高价值用户标签", "tag-profiling"),   # 标签/高价值 → tag_query
        ("评估贷款申请风险", "risk-control"),       # 贷款/风险 → risk_control
        ("训练CTR预测模型", "model-factory"),       # 训练/模型 → model_training
        ("同步CRM数据", "data-steward"),            # 同步/数据 → data_governance
        ("广告投放优化方案", "dmp-ad"),              # 广告/投放 → ad_optimization
        ("单纯数据查询", "insight-report"),         # 分析但无广告 → data_analysis
    ]

    results = []
    for user_input, expected_agent in test_cases:
        user_input_lower = user_input.lower()

        # 模拟意图识别
        matched = False
        for keywords, agent_name, intent_name in routing_rules:
            for keyword in keywords:
                if keyword in user_input_lower:
                    matched = True
                    matched_agent = agent_name
                    break
            if matched:
                break

        if not matched:
            matched_agent = "unknown"

        passed = matched_agent == expected_agent
        results.append((user_input, expected_agent, matched_agent, passed))

        status = "✓" if passed else "✗"
        logger.info(f"  {status} \"{user_input}\" → expected:{expected_agent}, got:{matched_agent}")

    passed = sum(1 for _, _, _, p in results if p)
    total = len(results)
    logger.info(f"\nIntent recognition: {passed}/{total} tests passed")
    return passed == total


def main():
    """主测试函数"""
    logger.info("=" * 50)
    logger.info("Data Platform Team Setup Test")
    logger.info(f"Time: {datetime.now().isoformat()}")
    logger.info("=" * 50)

    results = {}

    # 检查各组件
    results["directory_structure"] = check_directory_structure()
    results["config_files"] = check_config_files()
    results["skills"] = check_skills()
    results["mcp_servers"] = check_mcp_servers()
    results["agents"] = check_agents()
    results["intent_recognition"] = test_orchestrator_intent_recognition()

    # 输出结果汇总
    logger.info("\n" + "=" * 50)
    logger.info("SUMMARY")
    logger.info("=" * 50)

    passed = sum(1 for v in results.values() if v)
    total = len(results)

    for name, result in results.items():
        status = "✓ PASS" if result else "✗ FAIL"
        logger.info(f"  {name}: {status}")

    logger.info(f"\nTotal: {passed}/{total} checks passed")

    if passed == total:
        logger.info("\n✓ All checks passed! Project structure is valid.")
        logger.info("\nNext steps:")
        logger.info("  1. Install dependencies: pip install -r requirements.txt")
        logger.info("  2. Configure environment variables")
        logger.info("  3. Run individual MCP servers: cd mcp-servers/<server> && python server.py")
        logger.info("  4. Create team: Use TeamCreate to instantiate agents")
    else:
        logger.warning(f"\n✗ {total - passed} checks failed. Please review missing files.")

    return passed == total


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
