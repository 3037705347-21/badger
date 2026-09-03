# T01 Verify Rubric 追溯矩阵

结论：完成

- 独立 verifier：`/Users/zhangchengcheng/work/ai-project/Trea/verifiers/badger-range-delete-T01/delete_range_external_test.go`
- 命令：`go test -count=1 -v -timeout=5m`（exit 0）
- 原始输出：`.swe-evidence/independent_verifier.log`

## 冻结 Rubric

- **R1**：提供传入起止键的区间删除 API，调用后立即返回，不阻塞整个库
- **R2**：区间内的键在迭代器中不再可见
- **R3**：已删除空间在压缩/GC 后逐步回收，墓碑正确下沉
- **R4**：与单键删除、前缀删除、事务并发行为保持一致
- **R5**：空区间或不影响其他键时行为正确

## 原子行为与证据

| 原子行为 | 输入/前置条件 | 精确预期 | 实际结果 | verifier 证据 | 源码消费链路 | 结论 |
|---|---|---|---|---|---|---|
| **R1-A1**<br>提供传入起止键的区间删除 API，调用后立即返回，不阻塞整个库 | 向包含 50,000 个 bulk-* 键的 DB 调用 DB.DeleteRange([bulk-000000, bulk-999999))。 | API 接受起止键并在 2 秒内返回，而不是逐键同步删除。 | 独立 verifier 在时限内返回并通过。 | `/Users/zhangchengcheng/work/ai-project/Trea/verifiers/badger-range-delete-T01/delete_range_external_test.go:118`；TestExternalDeleteRangeReturnsPromptlyAndDoesNotBlockWriter PASS; verifier package PASS (1.583s; EXIT_CODE=0). | api: rangedel.go:288-308；transaction_entry: txn.go:452-501 | pass |

- **R1-A1 反证用例：** 若实现逐键同步遍历，verifier 在 50,000 键场景超过 2 秒即失败（delete_range_external_test.go:140-142）。

| **R1-A2**<br>提供传入起止键的区间删除 API，调用后立即返回，不阻塞整个库 | DeleteRange 执行期间并发提交范围外键 writer-outside-range。 | 范围外 writer 在 2 秒内完成，证明调用未阻塞整个库。 | 独立 verifier 收到 writer 成功结果并通过。 | `/Users/zhangchengcheng/work/ai-project/Trea/verifiers/badger-range-delete-T01/delete_range_external_test.go:118`；TestExternalDeleteRangeReturnsPromptlyAndDoesNotBlockWriter PASS; verifier package PASS (1.583s; EXIT_CODE=0). | api: rangedel.go:303-308；write_path: db.go:862-870 | pass |

- **R1-A2 反证用例：** 若 DeleteRange 持有整库写锁，writer 的 2 秒等待会超时并失败（delete_range_external_test.go:143-150）。

| **R2-A1**<br>区间内的键在迭代器中不再可见 | 写入 a,b,c,d,e 后删除 [b,e)，以正向、反向和 AllVersions 迭代。 | Get(b/c/d) 均为 ErrKeyNotFound；迭代结果仅为 a,e，反向仅为 e,a，AllVersions 同样仅为 a,e。 | 独立 verifier 的精确键列表与 Get 断言全部通过。 | `/Users/zhangchengcheng/work/ai-project/Trea/verifiers/badger-range-delete-T01/delete_range_external_test.go:90`；TestExternalRangeVisibilityAndBoundaries PASS; verifier package PASS (1.583s; EXIT_CODE=0). | tombstone_index: rangedel.go:34-111；get_filter: db.go:811-841；iterator_filter: iterator.go:640-648 | pass |

- **R2-A1 反证用例：** 若只写标记而读取/迭代器不消费它，b/c/d 会出现在 Get 或任一迭代器断言中而失败（delete_range_external_test.go:99-106）。

| **R3-A1**<br>已删除空间在压缩/GC 后逐步回收，墓碑正确下沉 | 将 80 个 64KiB value-log 值写入磁盘 DB，删除 [gone-, gone.)，重开后以公开 RunValueLogGC(0.01) 重试最多 12 次。 | 后台 L0 压缩产生可回收 discard stats；GC 成功且 .vlog 总字节严格下降；重开后 gone-* 仍不可见而 keep-key 保留。 | 独立 verifier 通过全部 GC、磁盘字节和重开可见性断言。 | `/Users/zhangchengcheng/work/ai-project/Trea/verifiers/badger-range-delete-T01/delete_range_external_test.go:249`；TestExternalPersistenceCompactionAndGC PASS; verifier package PASS (1.583s; EXIT_CODE=0). | range_marker: value.go:43-48; value.go:895-901；compaction_consume: levels.go:819-830；range_propagation: levels.go:1304-1315; levels.go:1471-1482；reload: db.go:400-406 | pass |

- **R3-A1 反证用例：** 若墓碑未参与压缩或未记入 discard stats，GC 不成功或 .vlog 字节不下降，断言会失败（delete_range_external_test.go:308-336）。

| **R4-A1**<br>与单键删除、前缀删除、事务并发行为保持一致 | 在同一事务中验证先写后范围删、先范围删后重写及快照隔离。 | 晚于范围删的写入可见；早于范围删的写入被删除；既有只读快照保留删除前观察语义。 | 独立 verifier 的事务与快照断言通过。 | `/Users/zhangchengcheng/work/ai-project/Trea/verifiers/badger-range-delete-T01/delete_range_external_test.go:153`；TestExternalTransactionSnapshotAndExistingDeletionCompatibility PASS; verifier package PASS (1.583s; EXIT_CODE=0). | txn_api: txn.go:452-501；same_txn_visibility: txn.go:503-544；commit_filter: txn.go:663-676 | pass |

- **R4-A1 反证用例：** 若同事务操作顺序或 snapshot 版本未被消费，事务/快照的精确可见性断言会失败（delete_range_external_test.go:153-222）。

| **R4-A2**<br>与单键删除、前缀删除、事务并发行为保持一致 | 交叉检查现有 Delete、WriteBatch.DeleteRange 与 DropPrefix，并并发执行 DeleteRange 和范围外 point write。 | 单键删除仍删除指定键；WriteBatch 区间删生效；DropPrefix 生效；并发范围外键保持可读且迭代排序不损坏。 | 独立 verifier 的兼容性与并发断言全部通过。 | `/Users/zhangchengcheng/work/ai-project/Trea/verifiers/badger-range-delete-T01/delete_range_external_test.go:153`；TestExternalTransactionSnapshotAndExistingDeletionCompatibility PASS; verifier package PASS (1.583s; EXIT_CODE=0). | batch_api: batch.go:173-193；drop_all: db.go:1883-1885；drop_prefix: db.go:1949-1953；concurrent_registration: db.go:862-870 | pass |
| **R5-A1**<br>空区间或不影响其他键时行为正确 | 对不存在范围 [z,zz)、相等范围 [same,same) 和反向范围 [z,a) 调用 DeleteRange，并检查范围外 middle；同时检查半开边界 a/e。 | 不存在范围不影响 middle；相等/反向范围返回 ErrInvalidRequest；[b,e) 保留 a/e，确保不误删范围外键。 | 独立 verifier 的边界、错误和保护键断言全部通过。 | `/Users/zhangchengcheng/work/ai-project/Trea/verifiers/badger-range-delete-T01/delete_range_external_test.go:339`；TestExternalEmptyAndInvalidRanges PASS; verifier package PASS (1.583s; EXIT_CODE=0). | validation: txn.go:473-501；coverage: rangedel.go:78-111；get_filter: db.go:811-841 | pass |

- **R5-A1 反证用例：** 若等值或反向边界被当作有效删除，或半开边界错误，保护键/错误断言将失败（delete_range_external_test.go:99-114, 339-352）。


所有 required atom 均由 Trae 工作区外的 verifier 实际执行并通过；每个核心 atom 的反证条件均为 verifier 中已经执行的负向/精确断言。
