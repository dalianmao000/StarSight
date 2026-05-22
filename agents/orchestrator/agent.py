#!/usr/bin/env python3
"""
Orchestrator Agent

主编排 Agent，负责意图识别、任务拆解、路由分发、状态追踪和冲突仲裁。
是整个数据平台的控制中枢。
"""

import os
import sys
import json
import uuid
import logging
import asyncio
from typing import Any, Dict, List, Optional
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum

import yaml
from pydantic import BaseModel, Field

logger = logging.getLogger("orchestrator-agent")


# ========================================
# 数据模型
# ========================================

class TaskStatus(str, Enum):
    """任务状态"""
    PENDING = "pending"
    ROUTED = "routed"
    IN_PROGRESS = "in_progress"
    WAITING_HITL = "waiting_hitl"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class TaskPriority(str, Enum):
    """任务优先级"""
    LOW = "low"
    NORMAL = "normal"
    HIGH = "high"
    URGENT = "urgent"


class IntentType(str, Enum):
    """意图类型"""
    TAG_QUERY = "tag_query"
    DATA_ANALYSIS = "data_analysis"
    RISK_CONTROL = "risk_control"
    AD_OPTIMIZATION = "ad_optimization"
    MODEL_TRAINING = "model_training"
    DATA_GOVERNANCE = "data_governance"
    UNKNOWN = "unknown"


@dataclass
class Task:
    """任务模型"""
    task_id: str
    action: str
    payload: Dict[str, Any]
    from_agent: str = "user"
    to_agent: Optional[str] = None
    status: TaskStatus = TaskStatus.PENDING
    priority: TaskPriority = TaskPriority.NORMAL
    callback: Optional[str] = None
    deadline: Optional[datetime] = None
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "task_id": self.task_id,
            "action": self.action,
            "payload": self.payload,
            "from_agent": self.from_agent,
            "to_agent": self.to_agent,
            "status": self.status.value,
            "priority": self.priority.value,
            "callback": self.callback,
            "deadline": self.deadline.isoformat() if self.deadline else None,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "result": self.result,
            "error": self.error
        }


class IntentRecognitionResult(BaseModel):
    """意图识别结果"""
    intent: IntentType
    confidence: float = Field(ge=0.0, le=1.0)
    entities: List[str] = []
    suggested_actions: List[str] = []
    clarification_needed: bool = False
    questions: List[str] = []


# ========================================
# 路由规则
# ========================================

@dataclass
class RoutingRule:
    """路由规则"""
    condition_keywords: List[str]
    target_agent: str
    hitl_required: bool = False
    priority_boost: TaskPriority = TaskPriority.NORMAL


# ========================================
# Orchestrator Agent
# ========================================

class OrchestratorAgent:
    """主编排 Agent"""

    # 路由规则定义
    ROUTING_RULES: List[RoutingRule] = [
        RoutingRule(
            condition_keywords=["金融", "风控", "信用", "欺诈", "风险", "贷款", "信贷"],
            target_agent="risk-control",
            hitl_required=True
        ),
        RoutingRule(
            condition_keywords=["标签", "画像", "人群", "圈选", "用户分群", "高价值"],
            target_agent="tag-profiling"
        ),
        RoutingRule(
            condition_keywords=["训练", "模型", "预测", "机器学习", "AutoML", "特征"],
            target_agent="model-factory"
        ),
        RoutingRule(
            condition_keywords=["广告", "投放", "DMP", "定向", "归因", "Lookalike", "CTR", "CVR", "ROI"],
            target_agent="dmp-ad"
        ),
        RoutingRule(
            condition_keywords=["分析", "洞察", "报告", "查询", "NL2SQL", "自然语言", "SQL"],
            target_agent="insight-report"
        ),
        RoutingRule(
            condition_keywords=["数据质量", "ETL", "同步", "治理", "数据源", "同步"],
            target_agent="data-steward"
        ),
    ]

    # HITL 触发条件
    HITL_TRIGGERS = [
        {"type": "budget_adjustment", "keywords": ["预算", "调整", "增加预算"]},
        {"type": "model_publish", "keywords": ["上线模型", "发布模型", "模型上线"]},
        {"type": "data_export", "keywords": ["导出数据", "数据导出", "下载数据"]},
        {"type": "tag_delete", "keywords": ["删除标签", "下线标签"]},
        {"type": "risk_decision", "keywords": ["风控决策", "拒绝", "通过"]},
    ]

    def __init__(self, config_path: Optional[str] = None):
        self.config_path = config_path
        self.tasks: Dict[str, Task] = {}
        self.team_members: List[str] = [
            "data-steward",
            "tag-profiling",
            "model-factory",
            "dmp-ad",
            "risk-control",
            "insight-report"
        ]

    def load_config(self) -> Dict[str, Any]:
        """加载配置"""
        if self.config_path and os.path.exists(self.config_path):
            with open(self.config_path, 'r') as f:
                return yaml.safe_load(f)
        return {}

    # ========================================
    # 意图识别
    # ========================================

    def recognize_intent(self, user_input: str) -> IntentRecognitionResult:
        """
        识别用户意图

        Args:
            user_input: 用户输入的自然语言

        Returns:
            IntentRecognitionResult
        """
        user_input_lower = user_input.lower()
        matched_intents = []
        entities = []

        # 匹配意图
        for rule in self.ROUTING_RULES:
            for keyword in rule.condition_keywords:
                if keyword in user_input_lower:
                    try:
                        matched_intents.append(IntentType(rule.target_agent.replace("-", "_")))
                    except ValueError:
                        pass
                    entities.append(keyword)

        if not matched_intents:
            return IntentRecognitionResult(
                intent=IntentType.UNKNOWN,
                confidence=0.0,
                clarification_needed=True,
                questions=["您是需要进行数据分析、标签管理、广告优化还是风控查询？"]
            )

        # 取置信度最高的意图
        primary_intent = matched_intents[0]
        confidence = min(0.9, 0.5 + 0.1 * len(entities))

        return IntentRecognitionResult(
            intent=primary_intent,
            confidence=confidence,
            entities=entities,
            suggested_actions=[rule.target_agent for rule in self.ROUTING_RULES if any(k in user_input_lower for k in rule.condition_keywords)]
        )

    # ========================================
    # 任务分解
    # ========================================

    def decompose_task(self, user_input: str, intent_result: IntentRecognitionResult) -> List[Task]:
        """
        分解任务

        Args:
            user_input: 用户输入
            intent_result: 意图识别结果

        Returns:
            任务列表
        """
        tasks = []

        # 根据意图类型决定任务
        intent = intent_result.intent

        if intent == IntentType.DATA_ANALYSIS:
            # 数据分析任务
            tasks.append(Task(
                task_id=str(uuid.uuid4()),
                action="query_and_analyze",
                payload={
                    "user_input": user_input,
                    "intent": intent_result.intent.value,
                    "entities": intent_result.entities
                },
                to_agent="insight-report",
                priority=TaskPriority.NORMAL
            ))

        elif intent == IntentType.TAG_QUERY:
            # 标签查询任务
            tasks.append(Task(
                task_id=str(uuid.uuid4()),
                action="get_or_create_tag",
                payload={
                    "user_input": user_input,
                    "entities": intent_result.entities
                },
                to_agent="tag-profiling",
                priority=TaskPriority.NORMAL
            ))

        elif intent == IntentType.RISK_CONTROL:
            # 风控任务 - 可能需要多步
            tasks.append(Task(
                task_id=str(uuid.uuid4()),
                action="risk_assessment",
                payload={
                    "user_input": user_input,
                    "entities": intent_result.entities
                },
                to_agent="risk-control",
                priority=TaskPriority.HIGH,
                hitl_required=True if any(k in user_input for k in ["贷款", "信贷", "授信"]) else False
            ))

        elif intent == IntentType.AD_OPTIMIZATION:
            # 广告优化任务
            tasks.append(Task(
                task_id=str(uuid.uuid4()),
                action="ad_optimization",
                payload={
                    "user_input": user_input,
                    "entities": intent_result.entities
                },
                to_agent="dmp-ad",
                priority=TaskPriority.NORMAL
            ))

        elif intent == IntentType.MODEL_TRAINING:
            # 模型训练任务
            tasks.append(Task(
                task_id=str(uuid.uuid4()),
                action="train_model",
                payload={
                    "user_input": user_input,
                    "entities": intent_result.entities
                },
                to_agent="model-factory",
                priority=TaskPriority.NORMAL
            ))

        elif intent == IntentType.DATA_GOVERNANCE:
            # 数据治理任务
            tasks.append(Task(
                task_id=str(uuid.uuid4()),
                action="data_governance",
                payload={
                    "user_input": user_input,
                    "entities": intent_result.entities
                },
                to_agent="data-steward",
                priority=TaskPriority.NORMAL
            ))

        else:
            # 默认转发给 insight-report
            tasks.append(Task(
                task_id=str(uuid.uuid4()),
                action="general_query",
                payload={
                    "user_input": user_input
                },
                to_agent="insight-report",
                priority=TaskPriority.NORMAL
            ))

        return tasks

    # ========================================
    # 任务路由
    # ========================================

    def route_task(self, task: Task) -> str:
        """
        路由任务到目标 Agent

        Args:
            task: 任务

        Returns:
            目标 Agent 名称
        """
        if task.to_agent and task.to_agent in self.team_members:
            return task.to_agent

        # 基于动作和有效负载进行路由
        action = task.action.lower()
        payload = task.payload

        for rule in self.ROUTING_RULES:
            if any(k in action for k in [r.lower() for r in rule.condition_keywords]):
                return rule.target_agent
            if any(k in str(payload) for k in rule.condition_keywords):
                return rule.target_agent

        return "insight-report"  # 默认

    # ========================================
    # HITL 检查
    # ========================================

    def check_hitl_required(self, task: Task) -> bool:
        """
        检查是否需要 HITL 审批

        Args:
            task: 任务

        Returns:
            是否需要 HITL
        """
        # 风控相关默认需要 HITL
        if task.to_agent == "risk-control":
            return True

        # 检查触发条件
        for trigger in self.HITL_TRIGGERS:
            if any(k in str(task.payload) for k in trigger["keywords"]):
                return True

        return False

    # ========================================
    # 任务执行
    # ========================================

    async def execute_workflow(self, user_input: str) -> Dict[str, Any]:
        """
        执行完整工作流

        Args:
            user_input: 用户输入

        Returns:
            执行结果
        """
        logger.info(f"Received user input: {user_input}")

        # 1. 意图识别
        intent_result = self.recognize_intent(user_input)
        logger.info(f"Intent recognized: {intent_result.intent.value}, confidence: {intent_result.confidence}")

        # 2. 需要澄清？
        if intent_result.clarification_needed:
            return {
                "success": False,
                "clarification_needed": True,
                "questions": intent_result.questions,
                "intent": intent_result.intent.value
            }

        # 3. 任务分解
        tasks = self.decompose_task(user_input, intent_result)
        logger.info(f"Tasks decomposed: {len(tasks)} tasks")

        # 4. 路由并执行任务
        results = []
        for task in tasks:
            # 路由
            target_agent = self.route_task(task)
            task.to_agent = target_agent
            task.status = TaskStatus.ROUTED

            # HITL 检查
            if self.check_hitl_required(task):
                task.status = TaskStatus.WAITING_HITL
                results.append({
                    "task_id": task.task_id,
                    "status": "waiting_approval",
                    "message": f"任务需要人工审批后才能执行",
                    "target_agent": target_agent
                })
                continue

            # 执行
            task.status = TaskStatus.IN_PROGRESS
            result = await self._execute_single_task(task)
            results.append(result)

        # 5. 聚合结果
        return {
            "success": True,
            "intent": intent_result.intent.value,
            "task_count": len(tasks),
            "results": results,
            "summary": self._generate_summary(results)
        }

    async def _execute_single_task(self, task: Task) -> Dict[str, Any]:
        """
        执行单个任务（模拟）

        注意：实际实现中，这里会通过 Agent 间通信协议调用目标 Agent
        """
        logger.info(f"Executing task {task.task_id} on agent {task.to_agent}")

        # 模拟执行
        await asyncio.sleep(0.1)

        task.status = TaskStatus.COMPLETED
        task.result = {
            "message": f"Task {task.action} executed successfully on {task.to_agent}",
            "timestamp": datetime.now().isoformat()
        }

        return {
            "task_id": task.task_id,
            "status": "completed",
            "agent": task.to_agent,
            "result": task.result
        }

    # ========================================
    # 结果聚合
    # ========================================

    def _generate_summary(self, results: List[Dict[str, Any]]) -> str:
        """生成结果摘要"""
        completed = sum(1 for r in results if r.get("status") == "completed")
        waiting = sum(1 for r in results if r.get("status") == "waiting_approval")
        failed = sum(1 for r in results if r.get("status") == "failed")

        parts = []
        if completed > 0:
            parts.append(f"{completed} 个任务已完成")
        if waiting > 0:
            parts.append(f"{waiting} 个任务等待审批")
        if failed > 0:
            parts.append(f"{failed} 个任务失败")

        return "，".join(parts) if parts else "无任务执行"

    # ========================================
    # 任务管理
    # ========================================

    def get_task_status(self, task_id: str) -> Optional[Dict[str, Any]]:
        """获取任务状态"""
        task = self.tasks.get(task_id)
        return task.to_dict() if task else None

    def list_pending_tasks(self) -> List[Dict[str, Any]]:
        """列出待处理任务"""
        return [
            task.to_dict()
            for task in self.tasks.values()
            if task.status in [TaskStatus.PENDING, TaskStatus.ROUTED, TaskStatus.WAITING_HITL]
        ]

    def cancel_task(self, task_id: str) -> bool:
        """取消任务"""
        task = self.tasks.get(task_id)
        if task and task.status in [TaskStatus.PENDING, TaskStatus.ROUTED]:
            task.status = TaskStatus.CANCELLED
            task.updated_at = datetime.now()
            return True
        return False


# ========================================
# 入口
# ========================================

async def main():
    """测试入口"""
    agent = OrchestratorAgent()

    # 测试用例
    test_cases = [
        "帮我分析一下Q3广告投放效果，找出ROI最高的人群",
        "查询高价值用户标签覆盖了多少人",
        "评估这笔贷款申请的风险",
        "帮我训练一个CTR预测模型",
        "同步一下CRM系统的用户数据",
    ]

    print("\n=== Orchestrator Agent Test ===\n")

    for user_input in test_cases:
        print(f"Input: {user_input}")
        result = await agent.execute_workflow(user_input)
        print(f"Result: {json.dumps(result, ensure_ascii=False, indent=2)}\n")


if __name__ == "__main__":
    asyncio.run(main())
