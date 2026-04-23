# CLAUDE.md — Aeron C 项目工作规则

## 不可打破的第一规则：C 版行为以 Java 版为唯一基准

C 实现在 Aeron 项目里是 Java 版的 1:1 port，**Java 版是行为的定义源**。任何 C↔Java 偏差都视作 bug（除非 Java 已明确标注 C-only 差异）。

### 优先级（从高到低，不可颠倒）

1. **对齐 Java 行为**（状态机、消息语义、close 顺序、线程模型、常量值）
2. 代码正确性 / 内存安全
3. 性能优化
4. C 习惯写法 / 工程便利
5. 工期

前一条永远压后一条。

### 禁止项

- **禁止 band-aid 绕过差异**：`sleep`、魔法超时、硬编码的"等一下就好了"、非 Java 版的 fallback 分支、只为让测试绿的静默降级。band-aid 只会隐藏真正的偏差。
- **禁止默默改语义**：如果 C 因为语言/运行时差异无法 1:1 对齐（如 `on_unavailable_image` 的线程模型、`aeron_client_conductor_on_close` 的 drain 行为），必须显式记录在 `test-gap-report.md` "未对齐清单"里，附原因与修复方向，不能静默往下走。
- **禁止"先 disable 再忘"**：disabled 的测试必须在 md 里有对应条目说明 root cause 和 unblocker，不能只加 `DISABLED_` 前缀。

### 决策流程

遇到 C↔Java 不一致时：

1. 先读 Java 版对应类/方法，确认 Java 的**当前**行为（不是文档里的旧版本）
2. 比对 C 实现，找出差异点
3. 默认动作：把 C 改成 Java 的样子
4. 只有在 Java 行为本身对 C 无意义（JVM-only 机制，如 `CountersManager` 里的 Java object header）时才允许偏离——且必须记进 md

### 合理的偏离举例

- Java 用 `ReentrantLock`，C 用 `pthread_mutex_t` — 机制等价，不算偏离
- Java 的 `Aeron.close()` 自动 drain async cmd，C 的 `aeron_client_conductor_on_close` 不 drain — **这是偏离，必须记 md**（已记在 §4.3）

## 其他工作约定

- 新增单元/集成测试优先 mirror Java 的 test 文件结构，而不是按 C 习惯合成大文件
- Commit 粒度：优先一个主题一条 commit；Java 版一条 commit 拆成的多 PR 在 C 这边也尽量拆
- 改动涉及 cluster/archive 协议常量时，必须和 Java 的 schema 对照，schema 版本号按 `aeron_semantic_version_major` 校验兼容性

## 参考文档

- [test-gap-report.md](test-gap-report.md) — 迁移进度 + 未对齐清单（权威）
- Java 源：`~/aeron-java-ref/`（如有）或主 repo `git log` 里的 Java 提交
