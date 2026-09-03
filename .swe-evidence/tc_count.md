# T01 有效 TC 统计

## 结论

**有效 TC 总数：111**。

统计口径：按 Trae renderer 日志中的唯一 tooling 调用 UUID 去重。只纳入成功的 `readFile`（69 次）、`applyChatSnapshotPatch`（40 次）和 `listFolder`（2 次）。`createFile` 的两次调用与紧随的同文件补丁调用合并，避免重复计数。

- 完整 Trae Session ID：`538128058159683:1de70ad3b9554ee71b03e50735cbc4cc_6a991bfc95b8feb2c974ccc2.6a991bfc95b8feb2c974ccc5.6a991bfc95b8feb2c974ccc3:TraeCode CN.3.3.95.no_sid.no_ppe.T(2026/9/3 15:04:28)`
- Session ID：`6a991bfc95b8feb2c974ccc2`
- User message ID：`6a991bfc95b8feb2c974ccc3`
- Trace ID：`1de70ad3b9554ee71b03e50735cbc4cc`
- 原始日志：`/Users/zhangchengcheng/Library/Application Support/Trae CN/logs/20260903T091722/window9/renderer.log`

## 纳入明细

| 序号 | TC ID | 工具 | 行为 | 结果 | renderer.log 行号 |
|---:|---|---|---|---|---:|
| 1 | `e8a465ad-3f07-4c78-9483-cea893577026` | `listFolder` | 枚举文件夹 | 成功（code=0） | 1321–1323 |
| 2 | `62054452-5476-437e-83f4-7ba75cba1304` | `listFolder` | 枚举文件夹：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01` | 成功（code=0） | 1665–1670 |
| 3 | `0e0461e4-24ae-477c-87a3-cf5bbcef4b1f` | `readFile` | 读取文件：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/txn.go` | 成功（code=0） | 1703–1708 |
| 4 | `26f0983f-1616-4d22-90f1-9bce8d65d438` | `readFile` | 读取文件：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/structs.go` | 成功（code=0） | 1722–1727 |
| 5 | `9ba4e535-cd7f-4e13-b917-f0394ea56116` | `readFile` | 读取文件：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/value.go` | 成功（code=0） | 1745–1750 |
| 6 | `715b63fe-9732-4e1f-9508-3c2ab9699602` | `readFile` | 读取文件：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/db.go` | 成功（code=0） | 1766–1771 |
| 7 | `15a0ab79-6c36-4982-80b9-21f79af0da8b` | `readFile` | 读取文件：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/db.go` | 成功（code=0） | 1790–1795 |
| 8 | `ebf5f656-454b-4811-a12e-d3ef2b9066f6` | `readFile` | 读取文件：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/iterator.go` | 成功（code=0） | 2626–2631 |
| 9 | `727281f0-136a-4c44-9fd3-7c3f29792852` | `readFile` | 读取文件：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/levels.go` | 成功（code=0） | 2647–2652 |
| 10 | `b9bb4fa2-3f7f-42ee-abe2-4331edce7e65` | `readFile` | 读取文件：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/levels.go` | 成功（code=0） | 2668–2673 |
| 11 | `f10438d8-5336-494b-84bd-bc5de2cd2b90` | `readFile` | 读取文件：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/memtable.go` | 成功（code=0） | 2695–2700 |
| 12 | `b6e779f2-38a5-4689-9d37-06433a4bd8fd` | `readFile` | 读取文件：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/table/builder.go` | 成功（code=0） | 2716–2721 |
| 13 | `2789d390-898f-42c4-92db-73071f343f47` | `readFile` | 读取文件：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/y/iterator.go` | 成功（code=0） | 3739–3744 |
| 14 | `fb4a2700-61bc-414a-9fad-14b17fa767f4` | `readFile` | 读取文件：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/backup.go` | 成功（code=0） | 4177–4182 |
| 15 | `f02d6604-7d58-4d8d-ae9c-abfb44386d11` | `readFile` | 读取文件：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/stream_writer.go` | 成功（code=0） | 4208–4213 |
| 16 | `0c1ae4f9-8259-4d29-94a8-a9cd1d06e595` | `readFile` | 读取文件：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/merge.go` | 成功（code=0） | 4237–4242 |
| 17 | `cca8294e-0162-4317-9038-196f28ba5853` | `readFile` | 读取文件：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/errors.go` | 成功（code=0） | 4273–4278 |
| 18 | `b9ea115f-55f8-4d7d-a784-3436f127acee` | `readFile` | 读取文件：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/batch.go` | 成功（code=0） | 5251–5256 |
| 19 | `9f148bcd-0a0b-4045-8dc8-389db9d8f666` | `readFile` | 读取文件：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/test_extensions.go` | 成功（code=0） | 5384–5389 |
| 20 | `77646d29-5ab4-44b9-a7ad-90439368c43b` | `readFile` | 读取文件：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/db2_test.go` | 成功（code=0） | 6093–6098 |
| 21 | `6078ce29-0e4f-4054-b88b-527e9549001f` | `applyChatSnapshotPatch` | 应用编辑补丁：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/rangedel.go` | 成功（code=0） | 6477–6524 |
| 22 | `d7a54747-c01c-4d63-8743-c796616bdd11` | `readFile` | 读取文件：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/value.go` | 成功（code=0） | 6708–6713 |
| 23 | `67505ef1-8d78-45d1-b883-7d2f6a050266` | `applyChatSnapshotPatch` | 应用编辑补丁：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/value.go` | 成功（code=0） | 6729–6776 |
| 24 | `a56c377d-7f49-4615-9043-4dcae746aa1b` | `readFile` | 读取文件：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/value.go` | 成功（code=0） | 7360–7365 |
| 25 | `2f35c843-a4e4-4ee6-9249-d77a37e7ff9a` | `applyChatSnapshotPatch` | 应用编辑补丁：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/value.go` | 成功（code=0） | 7381–7427 |
| 26 | `857a58a9-08b6-4672-b8d9-1d4b1004c4df` | `readFile` | 读取文件：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/db.go` | 成功（code=0） | 7713–7718 |
| 27 | `fa6b2ccf-9c21-4b7d-ae16-2459ee2f68fe` | `applyChatSnapshotPatch` | 应用编辑补丁：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/db.go` | 成功（code=0） | 7734–7781 |
| 28 | `be56f8cb-1c1d-4b5a-8b88-7fc8a431b060` | `readFile` | 读取文件：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/db.go` | 成功（code=0） | 7952–7957 |
| 29 | `b4ebbee4-4351-40ed-85ed-9b23a20c365e` | `applyChatSnapshotPatch` | 应用编辑补丁：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/db.go` | 成功（code=0） | 7973–8019 |
| 30 | `846b4f6a-d708-4382-a4b5-6b6e428ac292` | `readFile` | 读取文件：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/db.go` | 成功（code=0） | 8078–8083 |
| 31 | `4697d535-0f6a-40c9-825a-475c0a91677e` | `applyChatSnapshotPatch` | 应用编辑补丁：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/db.go` | 成功（code=0） | 8099–8145 |
| 32 | `a655baf0-d7bd-4ae1-a597-a051633d18c0` | `readFile` | 读取文件：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/db.go` | 成功（code=0） | 8316–8321 |
| 33 | `ffffab89-94ff-4de2-b2c3-ae2bf070dc2e` | `applyChatSnapshotPatch` | 应用编辑补丁：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/db.go` | 成功（code=0） | 8337–8383 |
| 34 | `fa5864ac-7cea-47c3-999d-294e85f03822` | `readFile` | 读取文件：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/db.go` | 成功（code=0） | 8439–8444 |
| 35 | `1fbf46c5-15d1-48e1-9bf7-0f23872cf545` | `applyChatSnapshotPatch` | 应用编辑补丁：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/db.go` | 成功（code=0） | 8460–8506 |
| 36 | `8242d671-179f-4871-99da-c6b79187a1b8` | `readFile` | 读取文件：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/db.go` | 成功（code=0） | 8568–8573 |
| 37 | `be0bf71b-c2bb-4a35-9854-b501e68de7d1` | `applyChatSnapshotPatch` | 应用编辑补丁：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/db.go` | 成功（code=0） | 8589–8635 |
| 38 | `022d96b5-2e0f-4dc6-aa1f-eb1fc78d3951` | `readFile` | 读取文件：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/db.go` | 成功（code=0） | 8695–8700 |
| 39 | `10e02366-82e1-4291-a1e3-01174708e99c` | `applyChatSnapshotPatch` | 应用编辑补丁：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/db.go` | 成功（code=0） | 8716–8762 |
| 40 | `540a98b0-339a-4f08-a768-369632c31f22` | `readFile` | 读取文件：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/txn.go` | 成功（code=0） | 8823–8828 |
| 41 | `93d87a61-4da0-472a-a2e2-87723d305de3` | `applyChatSnapshotPatch` | 应用编辑补丁：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/txn.go` | 成功（code=0） | 8844–8891 |
| 42 | `4a8acec4-bbb6-4acd-bd02-4ed5943808e3` | `readFile` | 读取文件：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/txn.go` | 成功（code=0） | 8947–8952 |
| 43 | `24af6302-5bb0-4e5c-9d67-75c29b025350` | `applyChatSnapshotPatch` | 应用编辑补丁：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/txn.go` | 成功（code=0） | 8968–9014 |
| 44 | `e257905c-d9e4-41f1-aef3-63862fcd93c2` | `readFile` | 读取文件：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/txn.go` | 成功（code=0） | 9079–9084 |
| 45 | `c35c64ef-8a48-4ff6-a62d-b7fee21e050c` | `applyChatSnapshotPatch` | 应用编辑补丁：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/txn.go` | 成功（code=0） | 9100–9146 |
| 46 | `c2bf91a0-1c73-4dfa-9532-010e5c7c6598` | `readFile` | 读取文件：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/txn.go` | 成功（code=0） | 9205–9210 |
| 47 | `185713af-8c4f-4ce8-b86b-5cf36471e79f` | `applyChatSnapshotPatch` | 应用编辑补丁：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/txn.go` | 成功（code=0） | 9226–9274 |
| 48 | `fe57053a-c9ac-433d-b369-83d7483a39ae` | `readFile` | 读取文件：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/iterator.go` | 成功（code=0） | 9342–9347 |
| 49 | `83e76166-f9d1-41c4-a8e1-4a569d8721ac` | `applyChatSnapshotPatch` | 应用编辑补丁：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/iterator.go` | 成功（code=0） | 9363–9410 |
| 50 | `235346e6-dc22-445f-8401-b54d78483a1a` | `readFile` | 读取文件：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/levels.go` | 成功（code=0） | 9464–9469 |
| 51 | `0bd081e0-54a4-49c2-9a73-5ccb6758646f` | `applyChatSnapshotPatch` | 应用编辑补丁：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/levels.go` | 成功（code=0） | 9485–9532 |
| 52 | `6150fcbc-4cb6-42be-a003-7f0f8cc62ebf` | `readFile` | 读取文件：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/batch.go` | 成功（code=0） | 9596–9601 |
| 53 | `75afed17-46d6-420d-96bd-c924fb03b4d2` | `applyChatSnapshotPatch` | 应用编辑补丁：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/batch.go` | 成功（code=0） | 9617–9664 |
| 54 | `2e6870c9-9a23-4b14-b178-f6a43c1edd87` | `readFile` | 读取文件：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/value_test.go` | 成功（code=0） | 11028–11033 |
| 55 | `89292fdc-8c0e-4514-b3ba-aa90d0eb63d1` | `applyChatSnapshotPatch` | 应用编辑补丁：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/rangedel_test.go` | 成功（code=0） | 11293–11340 |
| 56 | `cf1727b2-acff-4b8b-9f2f-01fb48739970` | `readFile` | 读取文件：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/rangedel_test.go` | 成功（code=0） | 11399–11404 |
| 57 | `c0b5d7e5-916a-45cb-a8b7-4421059f5881` | `applyChatSnapshotPatch` | 应用编辑补丁：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/rangedel_test.go` | 成功（code=0） | 11420–11466 |
| 58 | `061baa80-dba3-438d-8e9e-a7803b3593d3` | `readFile` | 读取文件：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/level_handler.go` | 成功（code=0） | 12154–12197 |
| 59 | `570fa1e4-d24f-4e52-b82e-37fdbd22b934` | `readFile` | 读取文件：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/compaction.go` | 成功（code=0） | 12374–12379 |
| 60 | `0e5caa6d-32ab-4080-a4d8-660f0ae97f6c` | `readFile` | 读取文件：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/rangedel.go` | 成功（code=0） | 12713–12718 |
| 61 | `9ae78a5d-cd91-4337-a35d-f727cac6543f` | `applyChatSnapshotPatch` | 应用编辑补丁：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/rangedel.go` | 成功（code=0） | 12734–12780 |
| 62 | `e78d16b0-56a0-4587-a071-3098545f20a8` | `readFile` | 读取文件：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/rangedel.go` | 成功（code=0） | 12840–12845 |
| 63 | `26793817-dacc-4b70-900b-25cceaade5dd` | `applyChatSnapshotPatch` | 应用编辑补丁：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/rangedel.go` | 成功（code=0） | 12861–12907 |
| 64 | `1ae72313-94b7-4f0c-8c07-7c9c88b26565` | `readFile` | 读取文件：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/levels.go` | 成功（code=0） | 12961–12966 |
| 65 | `b6eb9524-d49e-4bb3-8980-48e0fc74942d` | `applyChatSnapshotPatch` | 应用编辑补丁：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/levels.go` | 成功（code=0） | 12982–13028 |
| 66 | `0c155ac5-672d-44c7-907b-94a6ecfe1911` | `readFile` | 读取文件：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/levels.go` | 成功（code=0） | 13112–13117 |
| 67 | `ec2a270b-dfbc-482a-937f-affc391eef56` | `applyChatSnapshotPatch` | 应用编辑补丁：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/levels.go` | 成功（code=0） | 13133–13179 |
| 68 | `6bdb9a9f-8cd2-40b1-9dc9-99271aa3487e` | `readFile` | 读取文件：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/structs.go` | 成功（code=0） | 14352–14357 |
| 69 | `97f979d4-ba26-480c-8d87-b38d15a6a1d4` | `readFile` | 读取文件：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/structs.go` | 成功（code=0） | 14368–14373 |
| 70 | `f3266975-1477-4778-821d-09937f15127a` | `readFile` | 读取文件：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/structs.go` | 成功（code=0） | 14646–14651 |
| 71 | `26b224a3-4df2-4033-8b4c-aa85714d4cf8` | `readFile` | 读取文件：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/structs.go` | 成功（code=0） | 14705–14710 |
| 72 | `1ec4a10b-dfc7-4e74-a1de-24bca835da25` | `applyChatSnapshotPatch` | 应用编辑补丁：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/structs.go` | 成功（code=0） | 14726–14773 |
| 73 | `49230e72-ce46-41a8-8ce0-022b9c53e975` | `readFile` | 读取文件：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/txn.go` | 成功（code=0） | 14944–14949 |
| 74 | `f42ec627-54e3-4e4c-a07e-1981b9f57af2` | `applyChatSnapshotPatch` | 应用编辑补丁：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/txn.go` | 成功（code=0） | 14965–15011 |
| 75 | `8f5830b8-be57-49ff-94ba-df3b4cb79842` | `readFile` | 读取文件：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/txn.go` | 成功（code=0） | 15182–15187 |
| 76 | `0929f558-3aac-408e-aec9-c9ed9da5b2c4` | `readFile` | 读取文件：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/txn.go` | 成功（code=0） | 15196–15201 |
| 77 | `c5826c4b-2d71-41c8-b06c-fa83faa24f66` | `readFile` | 读取文件：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/txn.go` | 成功（code=0） | 15221–15226 |
| 78 | `5119b6a6-413e-4a6e-8a5e-6de706009c3d` | `applyChatSnapshotPatch` | 应用编辑补丁：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/txn.go` | 成功（code=0） | 15242–15288 |
| 79 | `c816e6d1-be55-4e6f-8a70-211922589fee` | `readFile` | 读取文件：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/rangedel.go` | 成功（code=0） | 15462–15467 |
| 80 | `749d8d9f-88c1-4663-9847-cd6ac97adf0d` | `applyChatSnapshotPatch` | 应用编辑补丁：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/rangedel.go` | 成功（code=0） | 15483–15529 |
| 81 | `034b7a2c-bd93-45c9-b817-81107258a884` | `readFile` | 读取文件：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/rangedel.go` | 成功（code=0） | 15601–15606 |
| 82 | `11a1e4fe-8f3c-4f70-a78d-8c07fddea6ff` | `applyChatSnapshotPatch` | 应用编辑补丁：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/rangedel.go` | 成功（code=0） | 15622–15668 |
| 83 | `4caf8174-d8cd-4d27-9281-9bbc8bb90a4d` | `readFile` | 读取文件：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/txn.go` | 成功（code=0） | 15727–15732 |
| 84 | `0a401d9f-6bef-4030-af1c-de5eb15fac09` | `applyChatSnapshotPatch` | 应用编辑补丁：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/txn.go` | 成功（code=0） | 15748–15794 |
| 85 | `5838093f-ff88-4759-a9a7-74eac3dc98ef` | `readFile` | 读取文件：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/txn.go` | 成功（code=0） | 15877–15882 |
| 86 | `345ef61d-8f77-4a96-b6fb-049204a4e96c` | `readFile` | 读取文件：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/txn.go` | 成功（code=0） | 15891–15896 |
| 87 | `24975b5a-0071-4b03-be01-339ecddeef90` | `readFile` | 读取文件：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/txn.go` | 成功（code=0） | 15916–15921 |
| 88 | `34b95ba4-0b58-4ba4-b72a-e6ace3a849f4` | `applyChatSnapshotPatch` | 应用编辑补丁：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/txn.go` | 成功（code=0） | 15937–15983 |
| 89 | `4e9eb57b-0acb-465d-8a27-58d7e6073d9c` | `readFile` | 读取文件：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/txn.go` | 成功（code=0） | 16042–16047 |
| 90 | `9076cbd0-d99d-43df-ba68-905746e99507` | `readFile` | 读取文件：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/txn.go` | 成功（code=0） | 16063–16068 |
| 91 | `f401f245-f8af-475d-a30e-4e284fcf4045` | `readFile` | 读取文件：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/txn.go` | 成功（code=0） | 16266–16271 |
| 92 | `d6ce7bf2-8d83-44d0-946b-6fb4ca330cfb` | `applyChatSnapshotPatch` | 应用编辑补丁：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/txn.go` | 成功（code=0） | 16287–16333 |
| 93 | `bd8eb4d1-568e-409c-bf43-c4651d2a4c4c` | `readFile` | 读取文件：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/txn.go` | 成功（code=0） | 16391–16396 |
| 94 | `c17d9dff-291a-4d11-961a-42575bd33139` | `applyChatSnapshotPatch` | 应用编辑补丁：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/txn.go` | 成功（code=0） | 16412–16458 |
| 95 | `99245655-69c3-45ea-8138-825964c4baa5` | `readFile` | 读取文件：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/rangedel_test.go` | 成功（code=0） | 16825–16830 |
| 96 | `a349d135-e228-4f78-a4f5-560cdb688400` | `applyChatSnapshotPatch` | 应用编辑补丁：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/rangedel_test.go` | 成功（code=0） | 16880–16926 |
| 97 | `778dae3e-30db-43aa-98a0-5396cb49f5c2` | `readFile` | 读取文件：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/rangedel_test.go` | 成功（code=0） | 16994–16999 |
| 98 | `a75d4833-6a6b-432b-939a-5866491b84f3` | `applyChatSnapshotPatch` | 应用编辑补丁：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/rangedel_test.go` | 成功（code=0） | 17015–17058 |
| 99 | `986e19d6-914c-42fb-a566-7340ce049606` | `readFile` | 读取文件：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/rangedel_test.go` | 成功（code=0） | 17132–17137 |
| 100 | `24bc92f9-9ac3-41c7-8ebc-e5bfaac57d8b` | `applyChatSnapshotPatch` | 应用编辑补丁：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/rangedel_test.go` | 成功（code=0） | 17153–17199 |
| 101 | `71afda69-deae-443b-890a-70a8f02629c3` | `readFile` | 读取文件：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/rangedel_test.go` | 成功（code=0） | 17375–17380 |
| 102 | `894e4769-3f40-44a3-b563-97533d330dec` | `applyChatSnapshotPatch` | 应用编辑补丁：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/rangedel_test.go` | 成功（code=0） | 17396–17442 |
| 103 | `25d6d365-df29-4aee-984b-4925195a887c` | `readFile` | 读取文件：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/rangedel_test.go` | 成功（code=0） | 17648–17653 |
| 104 | `752f4616-f16a-4f51-8462-e1964acd652a` | `applyChatSnapshotPatch` | 应用编辑补丁：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/rangedel_test.go` | 成功（code=0） | 17669–17712 |
| 105 | `6742db8e-a76d-4a9f-a9cd-d76948fb3737` | `readFile` | 读取文件：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/txn.go` | 成功（code=0） | 17910–17915 |
| 106 | `c1259421-5177-4588-a31f-5912d1ffae85` | `applyChatSnapshotPatch` | 应用编辑补丁：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/txn.go` | 成功（code=0） | 17931–17977 |
| 107 | `4014556d-2c44-4d96-b0e3-c9b781c51baa` | `readFile` | 读取文件：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/metrics_test.go` | 成功（code=0） | 19576–19581 |
| 108 | `65cdcab9-28c5-4dd7-ab27-e24c1130268b` | `readFile` | 读取文件：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/rangedel.go` | 成功（code=0） | 19610–19615 |
| 109 | `3dd45dd7-14e5-4354-89da-851331c5d732` | `applyChatSnapshotPatch` | 应用编辑补丁：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/rangedel.go` | 成功（code=0） | 19631–19677 |
| 110 | `57582e4d-0416-44c1-9bbb-3d2c40e35005` | `readFile` | 读取文件：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/rangedel.go` | 成功（code=0） | 20015–20020 |
| 111 | `7e4125a7-e787-42c8-993c-89b2bc0aaf02` | `applyChatSnapshotPatch` | 应用编辑补丁：`/Users/zhangchengcheng/work/ai-project/Trea/swe/badger-range-delete-T01/rangedel.go` | 成功（code=0） | 20036–20082 |

## 排除说明

| 项目 | 原始次数 | 排除原因 |
|---|---:|---|
| `getAutoRunConfig` | 191 | 自动运行配置读取，不是实现动作 |
| `getRulesDetails` | 27 | 规则/配置读取，不是实现动作 |
| `getDocumentByUri` | 120 | 编辑后的自动文档刷新；含 2 次失败 |
| `getDiagnostics` | 118 | 诊断刷新，不是独立用户要求的实施动作 |
| `fileDiffCount` | 49 | diff 辅助刷新，不是独立实施动作 |
| `getAllAgentExtensions` | 1 | 自动能力/上下文初始化 |
| `readFile (2 failed)` | 71 | 未成功完成，不计有效 TC |
| `createFile (2)` | 2 | 与紧随的同文件 applyChatSnapshotPatch 合并，避免重复计数 |
| `Shell plan items` | — | 日志中仅有计划项，未找到可验证的实际服务端工具调用 ID 与执行结果 |

## 可复核性

明细保存了每个唯一 TC UUID、实际成功状态和完整 renderer.log 行号。`tc_count.json` 保存同一明细以及原始调用分布，供逐项复算。
