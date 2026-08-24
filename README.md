# User Onboarding Drip Campaign

用户注册后的分级 onboarding 邮件提醒流程。

## 场景

```
用户注册 → 发 Welcome Email → 等待激活
  ├─ 3 天内激活 → 结束,发总结邮件
  └─ 3 天未激活 → Reminder #1 → 7 天未激活 → Reminder #2 → 20 天未激活 → Win-back Email
```

每一级等待都可能被用户激活的 signal 提前打断。Win-back 邮件如果最终也发送失败(重试耗尽),不会让整个 workflow 静默 `Failed`,而是走补偿分支标记 `needs_manual_followup`。包含 Timer、Signal、Query、Search Attribute、Retry Policy、Heartbeat、Saga 补偿 等 Temporal 特性。

## 技术栈

- Python + [`temporalio`](https://pypi.org/project/temporalio/) SDK
- `uv` 管理依赖
- `temporal server start-dev` 本地跑 Temporal Server + Web UI(`localhost:8233`)
- `pytest` + `pytest-asyncio`,配合 Temporal 的 time-skipping test environment 做测试

## 快速开始

```bash
# 装依赖
uv sync

# 起本地 Temporal server(另开一个终端,常驻)
temporal server start-dev

# 注册 search attribute(只需跑一次,幂等,重复跑安全)
uv run python register_search_attributes.py

# 起 worker(另开一个终端,常驻)
uv run python worker.py

# 触发一个 workflow
uv run python starter.py

# 可选:模拟用户点击激活链接(<workflow_id> 是 starter.py 打印出来的那个)
uv run python activate.py <workflow_id>

# 跑测试(time-skipping,几秒内验证完整 30 天的流程)
uv run pytest
```
