# MADP for OP

### 不同的 harness，共同的工作区，可核验的算子迭代。

[English](README.md) · [运行演示](docs/guides/FILE_COLLABORATION_DEMO.md) · [架构](docs/architecture/FILE_FIRST_COLLABORATION.md) · [证据边界](docs/showcase/EVIDENCE.md) · [纵向交错开发](docs/showcase/INTERLEAVED_OPERATOR_DEVELOPMENT.md)

MADP 是面向算子工程的**文件优先协作与执行核心**。我们把“模型 + 它的 harness
（工具、会话及运行环境）”视为独立主体，不要求所有参与者成为同一个 agent SDK 的子代理。

**协作单位是算子工作区，不是共享聊天记录。** 参与者保留自己的工具，通过源码、
case、实验记录、测试请求和原始结果交接工作。MADP 不替代 agent，不提供新推理模型；
它负责围绕研究工作的执行与证据边界。

最新已发布：[File-first Collaboration / 5.6.1a1](https://github.com/LoonyReina/MADP-for-OP/tree/preview-2026-10-07-interleaved-collaboration)。
其上一个版本是 [V5 Iteration Runtime / 5.6.0a1](https://github.com/LoonyReina/MADP-for-OP/tree/preview-2026-09-13-v5-iteration-runtime)。
公开仓库是核心预览，不是完整私有 AscendOP 部署。

## 为什么值得做

算子开发不只是生成代码，还要反复回答：哪里算错了、如何设计能区分假设的输入、
为什么本地快而外部评测不快。线索可能来自不同模型、工具或人工分析。
更换参与者，不应该意味着重讲上下文、重复提交测试、丢失失败版本。

- **可接力**：每题一个长期工作区，保存阶段、case 矩阵、基线、实验依据和下一步。
- **可追溯**：请求、执行、结果接收与 ACK 分开记录；聊天中的“成功”不代替原始结果。
- **保留研究自由**：Agent 决定实验、合法 case 修订、阶段和提交/暂缓；框架负责授权范围内的执行、回收与真实性校验。

同一候选一次只有一个写入者；不同算子独立推进。可以用 daemon 调度，也可以由
独立主线程通过 Gateway 手动续接，不要求 daemon 直接给每种 harness 发消息。
本地 PASS 不等于外部评测 PASS。

## 五分钟体验

Python 3.11+ 和独立虚拟环境，无需模型密钥、NPU/GPU 或比赛账号。

```bash
python -m venv .venv
# 按当前 shell 的方式激活 .venv，然后执行：
python -m pip install ./packages/ascendop_protocol ./packages/ascendop_control ./packages/ascendop_agent_runner ./packages/ascendop_daemon ./packages/ascendop_test_gateway
python scripts/demo_file_collaboration.py --root artifacts/file-demo-01
```

两个模拟参与者进程完成“错误候选 → 文件交接 → 修复候选”，实际使用公开 Gateway
的请求日志与证据接收。重新创建 Gateway 后读取已接收结果，不重提请求，再单独 ACK。
外部提交保持 HOLD。

预期 `1/3 → 3/3`，两轮证据都保留。**验证的是协议，不是两个真实模型的推理能力。**
每次用新的输出目录。详见[演示说明](docs/guides/FILE_COLLABORATION_DEMO.md)。

## 已验证与未开放

第二版通过 332 项源码测试和 332 项安装包测试，包含五个包；独立 Windows 测试不调用
模型、设备或外部评测。新版记录见[候选版本说明](release/file-first-collaboration/README.md)。

私有部署的 Codex、Kimi、DSH 协作提供了经验，包括人工续接与排障，但不等于公开版本
有三者的开箱即用集成。具体 GP/Engine 部署、硬件执行器、算子代码/case、账号与机器
配置不公开；部分旧策略接口仍需迁移，不把设计文档当作已实现能力。

新版公开了进一步脱敏的 Kimi–DSHarness–Codex 协作记录，并用一个可运行的
Markov toy model 解释为什么正确性、性能、诊断和交接需要纵向交错，而不是固化成
两个互不相见的流水线。模型是方法展示，不是硬件或模型性能宣称。

参阅[项目定位](docs/showcase/PROJECT_POSITIONING.md)、[路线图](docs/showcase/ROADMAP.md)、
[交接指南](docs/guides/PARTICIPANT_HANDOFF.md)、[协作记录](docs/showcase/COLLABORATION_RECORD_KIMI_DSHARNESS_CODEX.md)、
[跨 agent wiki 计划](docs/showcase/CROSS_AGENT_WIKI.md)和[证据清单](docs/showcase/EVIDENCE.md)。

Apache-2.0；提及模型和工具不代表获得其提供方背书。
