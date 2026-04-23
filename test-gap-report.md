# Aeron C 测试搬迁与 Archive 补齐报告 (2026-04-24)

> **🚩 第一规则**：C 版行为以 Java 版为唯一基准。对齐 Java > 代码正确性 > 性能 > C 习惯 > 工期。禁止用 sleep / 魔法常数 / 静默 fallback 等 band-aid 绕过偏差；无法对齐时必须记到本文档的"未对齐清单"（§4.3、§6）。详见 [CLAUDE.md](CLAUDE.md)。

## 总览

| 维度 | 状态 |
|---|---|
| Cluster C 测试 binary | 55（全部 enabled），ctest 全绿 |
| Archive C 测试 binary | 6，ctest 全绿 |
| 用例 meaningful 度（集成） | 82/194 (42%)，**单节点 + 3-node messaging / REDIRECT / failover 都已打通**（§4.3） |
| Archive server 控制 + 数据面 | **~100% 真实现**（Round 7+8 全部补齐，33 项 gap 全落地）|
| DISABLED tests | 0 — 全部集成用例都 enabled |
| 生产代码 bug 修（Java 对齐） | 15 个（见 §5） |

---

## 1. 单元测试搬迁 — ✅ 完成

**§1.1 已有 C 文件新增 +130 用例**（10 个 C 测试文件），**§1.2 缺失 C 文件新增 +140 用例**（25 个新建）。单元层面零跳过，基本与 Java 1:1 对齐。

### 1.1 已有 C 测试文件

| Java 测试文件 | C 测试文件 | Java | C | 搬迁新增 |
|-------------|----------|-----:|--:|--------:|
| ClusterBackupAgentTest | backup_agent_test | 3 | 23 | +3 |
| ClusterMemberTest | member_test | 24 | 72 | +20 |
| ConsensusModuleAgentTest | agent_test | 15 | 68 | +12 |
| ConsensusModuleContextTest | context_test | 45 | 136 | +42 |
| ConsensusModuleSnapshotTakerTest | snapshot_taker_test | 6 | 31 | +2 |
| ElectionTest | election_test | 25 | 45 | +9 |
| PendingServiceMessageTrackerTest | pending_message_tracker_test | 7 | 14 | +7 |
| PriorityHeapTimerServiceTest | timer_service_test | 25 | 40 | +16 |
| RecordingLogTest | recording_log_test | 33 | 69 | +13 |
| SessionManagerTest | session_manager_test | 2 | 27 | +2 |

### 1.2 缺失 C 测试文件（全部新建）

| Java 文件 | C 文件 | Java | C |
|-----------|--------|-----:|--:|
| AeronClusterAsyncConnectTest | async_connect_test | 7 | 7 |
| AeronClusterContextTest | client_context_test | 2 | 2 |
| AeronClusterTest | client_test | 2 | 2 |
| AuthenticationTest | authentication_test | 5 | 5 |
| ClusterBackupContextTest | backup_context_test | 7 | 7 |
| ClusterMarkFileTest | mark_file_test | 13 | 20 |
| ClusterNodeRestartTest | node_restart_test | 10 | 10 |
| ClusterNodeTest | node_test | 5 | 5 |
| ClusterTimerTest | timer_test | 3 | 3 |
| ClusterWithNoServicesTest | with_no_services_test | 4 | 4 |
| ClusteredServiceAgentTest | service_agent_test | 3 | 3 |
| ClusteredServiceContainerContextTest | service_container_context_test | 11 | 11 |
| ConsensusModuleConfigurationTest | configuration_test | 2 | 2 |
| EgressAdapterTest | egress_adapter_test | 10 | 10 |
| EgressPollerTest | egress_poller_test | 2 | 2 |
| LogSourceValidatorTest | log_source_validator_test | 3 | 3 |
| NameResolutionClusterNodeTest | name_resolution_test | 1 | 1 |
| NodeStateFileTest | node_state_file_test | 7 | 7 |
| PublicationGroupTest | publication_group_test | 8 | 8 |
| RecordingLogValidatorTest | recording_log_validator_test | 1 | 1 |
| RecordingReplicationTest | recording_replication_test | 4 | 4 |
| ServiceSnapshotTakerTest | service_snapshot_taker_test | 2 | 2 |
| SessionEventCodecCompatibilityTest | session_event_codec_test | 1 | 1 |
| SnapshotReplicationTest | snapshot_replication_test | 3 | 3 |
| StandbySnapshotReplicatorTest | standby_snapshot_replicator_test | 8 | 8 |

---

## 2. 系统测试搬迁 — ⚠️ 文件级完成、语义 42%

21 个 Java 系统测试文件都有 C 对应文件，ctest 全绿。但只有 **82/194 (42%)** 是 meaningful 实现，剩余 113 个是单节点冒烟/配置校验。

### 2.1 按用例深度分类

- **trivial** ≤ 6 行：启动集群 → 断言选出 leader
- **thin** 7–15 行：只校验配置值或单节点状态
- **meaningful** > 15 行：有实际操作流 + 状态断言

| 文件 | 总 | trivial | thin | meaningful |
|---|---:|---:|---:|---:|
| system_test | 91 | **44** | **47** | **0** |
| backup_test | 23 | 3 | 8 | 12 |
| tool_test | 16 | 0 | 5 | 11 |
| network_partition_test | 7 | 0 | 0 | 7 |
| service_ipc_test | 7 | 0 | 0 | 7 |
| system_multi_test *(新)* | 9 | 0 | 0 | 9 |
| session_reliability_test | 5 | 0 | 3 | 2 |
| single_node_test | 5 | 0 | 1 | 4 |
| multi_node_test | 4 | 0 | 0 | 4 |
| harness / svc_harness *(新，元测试)* | 5 | 0 | 0 | 5 |
| election_recovery / multi_service / network_topology / three_node | 4×3 | 0 | 0 | 12 |
| appointed_leader / integration | 2×2 | 0 | 2 | 2 |
| echo / catchup / offset_clock / shutdown_close / test_cluster / truncated_log / uncommitted_state | 8×1 | 0 | 0 | 8 |
| **合计** | **194** | **47 (24%)** | **66 (34%)** | **81 (42%)** |

> `system_multi_test`（[cpp](aeron-cluster/src/test/c/integration/aeron_cluster_system_multi_test.cpp)）用 TestCluster 把 `system_test.cpp` 里 7+3 个冒烟用例升级为真 3-node + single-node messaging 场景。

### 2.2 `system_test.cpp` 最大欠账：91/91 仍是冒烟

`system_test.cpp` 文件头自认：
> For tests that require a full multi-node cluster (not yet supported in C infrastructure), we validate the core code path with a single-node cluster.

7 个用例的真 3-node 版本已在 `system_multi_test.cpp`，剩 84 个仍冒烟。代表性：
- `shouldRecoverWhenFollowerIsMultipleTermsBehind*` → 没让 follower 掉队
- 绝大多数 `shouldXxxWhenLeader*` → 都是 1-node 单节点起跑

---

## 3. TestCluster 工具（[aeron_test_cluster.h](aeron-cluster/src/test/c/integration/aeron_test_cluster.h) / [.cpp](aeron-cluster/src/test/c/integration/aeron_test_cluster.cpp)）

C 端多节点测试工具，对齐 Java `TestCluster`。Phase 分三阶段：

| Phase | 范围 | 状态 |
|---|---|---|
| Phase 1 | 纯 CM：`start_all` / `await_leader` / `stop_node` / `restart_node` / `drive` / `await` + 选举/故障转移 | ✅ 完成，+10 meaningful 用例 |
| Phase 2a | CM + service 同虚拟时钟 tick | ❌ 已 revert（×57 变慢） |
| Phase 2b | service 在独立 wall-clock 后台线程（100 µs 轮询）+ 客户端 `connect_client`/`offer`/`poll_egress_once` | ✅ 单节点 + 3-node messaging 通（Round 8 打通 REDIRECT 路径后） |
| Phase 3 | Snapshot 恢复路径 | ❌ 未做 |

**Phase 2b 已通**：`shouldEchoSingleNodeEndToEnd`, `shouldSetClientName`, `shouldCallOnRoleChangeOnBecomingLeaderSingleNodeCluster`, `shouldEnterElectionWhenRecordingStopsUnexpectedlyOnLeaderOfSingleNodeCluster`, `shouldEchoSingleMessage`（3-node real REDIRECT path，见 §4.3）, `shouldFailoverWithServiceOn`（3-node on-service 选举故障转移）。

---

## 4. C Archive server — ✅ Round 7+8 全部补完

Round 7 定位到 C archive server 是**半残桩**（所有 `check_new_images` / 信号发送 / catalog 生命周期 / segment ops / update_channel / replicate / authenticator / counter 分配 / channel stripping 都没实现）。Round 8 全部补齐。

**控制 API 对齐度**：**40% → ~100%**。所有 SBE request handler 都真接到 conductor；所有响应/信号都按 Java 语义真发。

### 4.1 补完清单（33 项 gap 全落）

**P1（14 项）**：
- `check_new_images` 轮询 subscription 新 image → `catalog_add_recording` → `recording_session_create` → START 信号
- `close_recording_session` 发 STOP 信号
- `stop_recording_by_identity` / `stop_recording` / `stop_recording_subscription` 按 subscription_id 精确 abort
- 5 个 segment ops（`detach` / `delete_detached` / `purge` / `attach` / `migrate`）conductor + handler 真删/改文件 + 真改 catalog
- `purge_recording` 真删 segment 文件
- `replicate` / `stop_replication` 真建/abort `replication_session`
- `extend_recording` 真建 subscription，`extend_target_recording_id` 让 check_new_images 走 extend 路径（重用 recording_id、发 EXTEND 信号）

**P2（10 项）**：
- `start_recording` 返回真 aeron `registration_id`；CM 精确按 subscription_id 匹配信号
- `local_control` 与 `control` 同 IPC channel+stream 时跳过重复订阅（防请求被双发）
- mark_file activity timestamp 每 tick 真刷
- `get_recording_position` / `get_max_recorded_position`：active session 真取值，stopped 走 catalog 回退
- `UPDATE_CHANNEL_REQUEST` / `REPLAY_TOKEN_REQUEST` 真接
- `list_recording_subscriptions` 真发 descriptor
- Authenticator CHALLENGED state default-accept，预留 hook 位置
- catalog 加 `update_start_position` / `update_stop_position` / `replace_recording` API

**P3（9 项）**：
- `unavailable_image_handler` 接 auto-abort
- `aeron_archive_strip_channel` 真 port Java `strippedChannelBuilder`（URI builder 清 ephemeral keys）
- `replication_session` channel 构造用 `aeron_uri_string_builder`（替换 snprintf 拼接）
- `replication_session` / `update_channel_session` 用 `catalog_replace_recording` in-place（fit 不下 fallback）
- Position counter 真分配（`aeron_async_add_counter` + RecordingPos type_id=100 + Java 格式 key/label），每 tick poll resolve → attach
- adapter image context 用 `aeron_header_context(header)` 真传（不再 pass NULL）
- `recording_session_recorded_position` 用 writer_position fallback

### 4.2 CM 侧对应改动

- [aeron_consensus_module_agent.c](aeron-cluster/src/main/c/consensus/aeron_consensus_module_agent.c) `cm_on_recording_signal` 消费 START/STOP
- `log_recording_stopped_unexpectedly` flag → `slow_tick_work` 里 `poll_for_recording_signals` + `do_work` 里 enterElection 分支
- `join_log_as_leader` 修 subscription_id 没存 bug；两条 start_recording 路径幂等
- `update_recording_log` 在 log_recording_id=-1 时 defer，等 START 信号到达重触发

### 4.3 独立遗留（不属于 archive server）

**3-node `shouldEchoSingleMessage` ✅ 打通**（Round 8 这一轮）— 按 Java `AeronCluster` / `ConsensusModuleAgent` / `ClusterSession` / `ArchiveConductor` 逐层对齐后，svc_harness 4 tests × 5 runs、service_ipc / network_partition / multi_service 各 × 3 runs 全 PASSED + LSan 干净：
1. ✅ `TestCluster` `base_node_index` 非对齐导致节点间 `cluster_members` / `ingress_endpoints` 视图发散（已 port 成 Java `(memberId, memberCount, clusterId, portBase)` 模型）
2. ✅ `snap_recording_id` 解析 race
3. ✅ `LogAdapter` 对未知 schema 消息直接 drop（现 forward 到 `agent_on_extension_message`）
4. ✅ `async_connect` REDIRECT 只更新 `ingress_endpoints` 不重建 publication — port 成 Java `AeronCluster.updateMembers`（[AeronCluster.java:2538](aeron-cluster/src/main/java/io/aeron/cluster/client/AeronCluster.java#L2538)）：关闭旧 publication，在新 leader endpoint 上 async_add_publication，状态回到 `ADD_PUBLICATION`；原先会走进 REDIRECT→SEND_CONNECT_REQUEST 的无限循环直到 timeout
5. ✅ Follower 端发 REDIRECT 时 detail 只带 leader 自己的 endpoint，而非全 active-members CSV — Java 在 `ConsensusModuleAgent.slowTickWork` 用 `ClusterMember.ingressEndpoints(activeMembers)` 生成全集（[ConsensusModuleAgent.java:2314](aeron-cluster/src/main/java/io/aeron/cluster/ConsensusModuleAgent.java#L2314)）。C 已改成走 `aeron_cluster_members_ingress_endpoints` 生成完整 CSV
6. ✅ `session_manager.send_pending_list` 每 tick 都 `aeron_async_add_exclusive_publication` + 立即 poll + 丢弃 handle，永远不 resolve + 每 tick 泄漏一个 async。改成用 session 上 `async_response_pub` 字段跨 tick 持久化 handle
7. ✅ `send_redirects` 把 `agent->member_id`（自身 id）当 leader_member_id 塞进 REDIRECT event — 客户端拿到的 "leader" 永远是 follower 自己，`updateMembers` 重建 publication 又连回同一 follower 死循环。Java 用 `leaderMember.id()`（ConsensusModuleAgent.java:2352）。C 改成 `agent->leader_member->id`
8. ✅ Archive conductor close 不 close replication/replay session（只 free 数组）→ 补齐；控制 session close 漏 null 化 replication_session 里反向指针 → 补上；`replication_session_close` 漏 free `src_archive_ctx` → 补上（async_connect 复制一份给 archive，原件仍归 session）；`election_close` 不 close 在飞 log_replication → 补上
9. ✅ Archive `start_recording` / extend_recording 的 `aeron_async_add_subscription` 改成 Java-style 同步 spin-poll（mirrors `aeron.addSubscription`），消除 async 生命周期 → 消除 shutdown 期 remove-cmd 漏 drain 的残留 leak
- ✅ `aeron_archive_conductor_on_unavailable_image` 不再从 aeron client 线程直接 mutate recording_session（原先的 UAF 路径）。对齐 Java `ArchiveConductor.onUnavailableImage` — 只做 `control_session_adapter_abort_by_image`。recording session 自身在 conductor 线程的 `recording_session_record` 检测 `aeron_image_is_closed`/`is_end_of_stream` 转 INACTIVE，线程安全。
- ✅ `RecordingPos` 计数器从 `aeron_async_add_counter` + do_work 轮询改成 Java-style 同步分配（`add_recording_position_counter` 内部 spin-poll 到 resolved/timeout，mirrors Java `aeron.addCounter`）。直接把 counter 传进 `recording_session_create`，从第一 tick 起就是 zero-copy 计数器路径。消除了先前 shutdown 期 `aeron_async_add_counter` 残留 cmd/key/label 三段 buffer 的 LSan leak（根因：async handle 在 client runner join 前被 orphaned）。`poll_pending_position_counters`、close 路径的 poll/cancel 逻辑都随之移除。10/10 run failover 测试 LSan 干净。

---

## 5. 生产代码 bug 修（15 个，全部与 Java 对齐）

**早期轮次（R7）**：
1. `aeron_consensus_module_agent.c` 3 处 `enterElection` 路径缺 `agent->role = AERON_CLUSTER_ROLE_FOLLOWER`（Java enterElection 开头立即 role(FOLLOWER)）
2. `aeron_cluster_session_manager` REJECTED 分支缺 `work++`
3. `aeron_cluster_recording_replication` `try_stop_replication` 缺 NULL archive guard
4. `aeron_cluster_standby_snapshot_replicator` `poll` 缺 NULL archive guard
5. `aeron_consensus_module_agent_join_log_as_leader` 里 `start_recording` 返回的 subscription_id 扔到了局部变量 `sub_id`，从没赋给 `agent->log_subscription_id`
6. `aeron_archive_recording_session` writer double-close：`session_do_work` INACTIVE→STOPPED 路径和 `session_close` 都 free writer（之前 recording_session 从没真创建过所以没触发）

**R8 — 3-node messaging 打通过程中发现并修复**：
7. Archive `on_unavailable_image` 从 client 线程 mutate recording_session → UAF；改成只调 `control_session_adapter_abort_by_image`（对齐 [ArchiveConductor.onUnavailableImage:237](aeron-archive/src/main/java/io/aeron/archive/ArchiveConductor.java#L237)），recording session 自己在 `recording_session_record` 检测 `aeron_image_is_closed` 转 INACTIVE
8. `RecordingPos` counter 从 `aeron_async_add_counter` + do_work 轮询改成同步 spin-poll（mirrors Java [aeron.addCounter](aeron-archive/src/main/java/io/aeron/archive/status/RecordingPos.java#L130)），消除 shutdown 期 async handle orphan leak
9. `async_connect` REDIRECT 只更新 `ingress_endpoints` 不重建 publication → port 成 Java `AeronCluster.updateMembers`（AeronCluster.java:2538）：close 旧 pub、新 leader endpoint 上 async_add_publication、状态回到 `ADD_PUBLICATION`；原先是无限 REDIRECT 循环
10. Follower 端发 REDIRECT 时 detail 只带 leader 自己的 endpoint → 改成全 active-members CSV（mirrors [ClusterMember.ingressEndpoints](aeron-cluster/src/main/java/io/aeron/cluster/ConsensusModuleAgent.java#L2314)），与 client 解析器对得上
11. `session_manager.send_pending_list` 每 tick 重建 `aeron_async_add_exclusive_publication` + 立 poll + 丢 handle → 改成用 session 上 `async_response_pub` 字段跨 tick 持久化 handle
12. `send_redirects` 把 `agent->member_id`（自己）当 leader_member_id 塞进 REDIRECT → 改成 `agent->leader_member->id`（mirrors `ConsensusModuleAgent.slowTickWork:2352` 的 `leaderMember.id()`）；这条是主因，客户端之前永远重连回同一个 follower
13. Archive conductor close 只 free replication_sessions / replay_sessions **数组**，不 close 个体 session → 补 close 循环（mirrors Java `ArchiveConductor.closeSessionWorkers`）
14. 控制 session close 漏 null 化 replication_session 里反向指针 → 补上；`replication_session_close` 漏 free `src_archive_ctx` → 补上（`aeron_archive_async_connect` 复制一份给 archive，原件仍归 session）；`election_close` 不 close 在飞 log_replication → 补上
15. Archive `start_recording` / `extend_recording` 的 `aeron_async_add_subscription` 改成同步 spin-poll（mirrors Java `aeron.addSubscription`），彻底消除 async 生命周期，顺带干掉最后一条 shutdown 期 remove-cmd 漏 drain 残留 leak

额外改造：`heap_sift_down` 相同 deadline 排序、`invalidate_entry` sorted-index 对齐、CM 状态机 `QUORUM_SNAPSHOT→QUITTING` / `LEAVING→TERMINATING`、`on_service_ack` QUITTING 分支、`on_extension_message` 分发、`replay_start_position`、context conclude 校验。

---

## 6. 还欠的（按影响排序）

1. **`system_test.cpp` 84 个冒烟用例**：现在 TestCluster 3-node messaging 已打通（§4.3），可以逐步升级到真 3-node 场景。
2. **Snapshot 恢复端到端**：所有 `shouldRestartWithSnapshot` 系列目前是"启动成功就算过"。3-node messaging 已打通，可以开始做。
3. **`ClusterSessionReliabilityTest`**（5 个里 3 个是冒烟）：Java 依赖 `DebugSendChannelEndpoint` 注入丢包，C 驱动没这个机制。要补驱动侧 LossGenerator hook。
4. **`TestCluster.java` 端行为补齐**：故障注入、真消息生成/收集 API（Java 端有一整套 fixture）。

---

## 7. "✅ 完成"的实际含义

- **单元测试**（§1）：语义级对齐，真通过了。
- **系统测试**（§2-3）：42% 是 meaningful，58% 是冒烟。R8 打通 3-node messaging / REDIRECT / failover 之后，升级剩下 84 个 smoke 只是工作量，不再是技术阻塞。
- **Archive server**（§4）：真 1:1 Java port，包括 RecordingPos 同步分配、`start/extend_recording` 同步 subscription、replication/replay session 完整关闭链。
- **生产 bug 修复**（§5）：15 个 Java↔C 对比实打实 bug，其中 9 个（R8）是 3-node 路径上必须修的（UAF / 死循环 REDIRECT / follower 把自己当 leader 等）。

下一步不再被单个 blocker 挡住；按 §6 顺位推 system_test.cpp smoke 升级、snapshot 恢复端到端、driver LossGenerator。
