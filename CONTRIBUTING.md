# 贡献指南

感谢您对数据服务平台项目的关注！我们欢迎各种形式的贡献，包括但不限于 Bug 修复、功能开发、文档改进等。

## 行为准则

- 使用清晰、有意义的提交信息
- 保持代码风格一致（遵循 black + isort 规范）
- 确保所有测试通过后再提交 PR
- 尊重其他参与者的意见和贡献

## 如何贡献

### 1. Fork & Clone

```bash
# Fork 本仓库到您的 GitHub 账户
# 然后克隆到本地
git clone https://github.com/your-username/p52_StarSight.git
cd p52_StarSight

# 添加上游仓库
git remote add upstream https://github.com/original-owner/p52_StarSight.git
```

### 2. 创建功能分支

```bash
# 确保基于最新代码
git checkout main
git pull upstream main

# 创建新分支
git checkout -b feature/your-feature-name
# 或
git checkout -b fix/issue-description
```

### 3. 开发 & 测试

```bash
# 安装开发依赖
pip install -e ".[dev]"

# 运行测试
pytest

# 代码格式化
black .
isort .

# 类型检查
mypy .
```

### 4. 提交代码

```bash
# 提交规范
# <type>: <subject>
#
# Types:
#   - feat: 新功能
#   - fix: Bug 修复
#   - docs: 文档更新
#   - style: 代码格式（不影响功能）
#   - refactor: 重构
#   - test: 测试相关
#   - chore: 构建/工具相关

git add .
git commit -m "feat: add new MCP server for Milvus"
```

### 5. 推送 & 创建 PR

```bash
# 推送分支
git push origin feature/your-feature-name

# 在 GitHub 上创建 Pull Request
# 填写 PR 模板（会自动生成）
```

## PR 模板

```markdown
## 描述
请简要描述您的更改

## 变更类型
- [ ] 新功能 (feature)
- [ ] Bug 修复 (fix)
- [ ] 文档更新 (docs)
- [ ] 代码重构 (refactor)
- [ ] 测试相关 (test)
- [ ] 其他

## 关联 Issue
Fixes #(issue number)

## 测试清单
- [ ] 本地测试通过
- [ ] 代码格式化检查通过
- [ ] 类型检查通过（如果有）
- [ ] 文档已更新（如果是新功能）

## 截图（可选）
如有 UI 变更，请提供截图
```

## 开发规范

### 代码风格

- 使用 **black** 格式化（line-length: 100）
- 使用 **isort** 排序 imports
- 遵循 **PEP 8** 规范
- 添加有意义的变量和函数命名

### 文件命名

```
# 模块命名：snake_case
data_steward/
├── agent.py
└── system.prompt

# 类命名：PascalCase
class OrchestratorAgent:

# 函数命名：snake_case
def execute_workflow():

# 常量命名：UPPER_SNAKE_CASE
MAX_RETRY_COUNT = 3
```

### Commit 规范

```
feat: add user profiling agent
fix: resolve database connection pool leak
docs: update API documentation
refactor: simplify task routing logic
test: add integration tests for orchestrator
chore: update dependencies
```

### 文档要求

- 新功能必须更新 README.md
- 复杂逻辑需要添加 docstrings
- API 变更需要更新 API 文档

## 问题反馈

如果发现 Bug 或有新功能建议：

1. 先搜索 [Issue 列表](https://github.com/your-repo/p52_StarSight/issues)，确认是否已存在
2. 创建新 Issue，选择对应的模板
3. 详细描述问题或建议，包括：
   - 复现步骤
   - 预期行为 vs 实际行为
   - 环境信息（Python 版本、操作系统等）
   - 相关日志或截图

## 许可

通过贡献代码，您同意将您的贡献按照项目的 [MIT License](../LICENSE) 条款发布。