# User Onboarding Drip Campaign

用户注册后的分级 onboarding 邮件提醒流程。

## 场景

```
用户注册 → 发 Welcome Email → 等待激活
  ├─ 3 天内激活 → 结束,发总结邮件
  └─ 3 天未激活 → Reminder #1 → 7 天未激活 → Reminder #2 → 20 天未激活 → Win-back Email
```

每一级等待都可能被用户激活的 signal 提前打断。包含 Timer、Signal、Query、Search Attribute、Retry Policy 等 Temporal 特性。

## 技术栈

- Python + [`temporalio`](https://pypi.org/project/temporalio/) SDK
- `uv` 管理依赖
- `temporal server start-dev` 本地跑 Temporal Server + Web UI(`localhost:8233`)

## 快速开始

```bash
# 装依赖
uv sync

# 起本地 Temporal server(另开一个终端,常驻)
temporal server start-dev
```
