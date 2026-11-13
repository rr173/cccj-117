# rvault — 本地研究资料接收与撤回处理程序

落实"参与者撤回授权后，从后续研究数据中消失"的本地工具：接收逐行 JSONL
批次、保存全部 edition 历史、为分析人员生成数据包（bundle），并以可分阶段、
可崩溃续办的方式执行撤回擦除。范围仅限工作区逻辑数据与普通 bundle 文件；
**不包含**用户账号、审批流等周边系统。

无第三方依赖，Python ≥ 3.9 标准库即可运行。

```
PYTHONPATH=. python3 -m rvault --store <存储目录> <命令> ...
```

一个 store 目录即系统管理的全部存储区，系统只向其中写入：

```
vault.sqlite3   逻辑状态（批次、edition 历史、撤回登记、bundle 登记）
bundles/        可读目录：已发出的 bundle 文件 bundles/<id>.json
tmp/            暂存区：来件 spool、半成品 bundle；此处内容永不视为可读输出
receipts/       批次回执（仅标识符，无姓名/测量/备注）
locks/          每 study 一个 flock 锁文件
```

---

## 1. 资料接收（import）

```
rvault --store S import --batch-tag T [--input FILE]     # 默认读 stdin
```

每行一个 JSON 对象，字段恰好为 `study, person, observation, edition,
display_name, measure, memo`：

- `study/person/observation` 为非空字符串；`edition` 为正整数；`measure` 为整数；
  `display_name/memo` 为字符串。缺字段、多字段、类型不符均算损坏行。
- **batch_tag 是不可拆分的交付凭据**：整批在一个 SQLite 事务中提交，任一行
  损坏（JSON 坏、字段错、observation 归属冲突、同号 edition 内容变更）则整批
  拒收，回执给出**每处**坏行位置（行号 + 原因），绝不因其它行可读而算成功。
- 同一 `study/observation` 的所有 edition 必须属于同一 `person`，冲突即拒批。
- **全部 edition 都保存**，查询以编号最高者为准；重复编号内容改变时拒收，
  哪怕该 edition 早已被更新版取代；内容完全相同的重复投递按幂等处理
  （`accepted_existing`，不重复计入）。
- **重复提交同一 batch_tag 不重复计入**：内容逐字节相同 → 返回 `duplicate`
  回执，且逐项按**当前撤回登记**重新标注（见 §4）；内容不同 → 拒收。
- **输入可大于可用内存**：来件先流式 spool 到 `tmp/`（同时计算摘要），再逐行
  在事务中应用，回执流式写入文件。实测 28 MB / 20 万行输入峰值 RSS 约 17 MB。

回执：`receipts/<batch_tag>-<摘要>.jsonl`，逐项一行（仅
line/study/observation/person/edition/disposition），stdout 打印汇总
（`accepted` / `accepted_existing` / `withdrawn_suppressed` 分列计数）。

## 2. 查询（query / history / aggregate）

```
rvault --store S query --study X                        # 现行投影：每 observation 采用最高 edition
rvault --store S history --study X --observation O      # 某 observation 的全部 edition
rvault --store S aggregate --study X [--groups G.json]  # 按 group_code 汇总人数与 measure_total
```

`--groups` 为 `{"person": "group_code"}` 映射文件，未列出者归入 `UNGROUPED`。
所有读路径都经过撤回屏障过滤（见 §4）。

## 3. 数据包（bundle）

```
rvault --store S bundle create --study X [--groups G.json]
rvault --store S bundle list [--all]
rvault --store S bundle read --bundle-id B000001
```

- 每份 bundle 含唯一单调编号（发出后永不复用、不被新内容覆盖）、参与者清单、
  逐 observation 采用的 edition 明细、按 group_code 的人数与 measure_total 汇总；
  **明细与汇总取自同一个读事务快照**。
- 文件先写入 `tmp/<id>.tmp` 并 fsync，再原子 rename 进 `bundles/`、登记为
  `active`；**未完成的临时文件不会出现在可读目录**。崩溃留下的
  `registering` 状态由 `reconcile` 清扫（各命令启动时也会自动执行）。
- `bundle read` 读回文件并校验登记摘要；已删除/未知编号报错（退出码 4）。

## 4. 撤回（withdraw）

```
rvault --store S withdraw register --withdrawal-id W --study X --person P [--person Q ...]
rvault --store S withdraw resume (--withdrawal-id W | --all)
rvault --store S withdraw status [--withdrawal-id W]
```

**分阶段提交**（每阶段独立事务，进度落库）：

1. `registered`：先写永久撤回登记与屏障（barrier）。**屏障一旦提交，检索、
   history、aggregate、新 bundle 申请立即隐藏这些人**——即使后续阶段尚未执行。
2. `editions_cleared`：从 edition 历史删除这些人的全部行（现行投影是历史之上
   的查询，随之消失）。observation 身份（归属 tombstone）与 batch_tag 摘要
   **故意保留**——它们不含测量内容，用来阻止旧数据复活。
3. 删除所有参与者清单中含这些人的**完整 bundle 文件**（绝不只改清单留明细）。
   逐个删除、逐个提交；文件已丢失记 `missing`；删除只按 bundle 编号推导出的
   规范路径 `bundles/<id>.json` 执行，**登记路径永不用于 unlink**——陌生路径、
   管理目录外内容绝不可能被按请求删除，路径不一致只记为 anomaly。

只有受影响资料与文件都不可再读，状态才置为 `completed`。

**撤回不是一条可被新版覆盖的 observation**：屏障登记后，此人后续所有 edition
（旧编号、新编号、新 observation）一律拒收载荷，同批其它参加者正常入列，
回执分列 `accepted` 与 `withdrawn_suppressed`；JSONL 结构坏、归属冲突仍拒整批。
允许先登记撤回、以后才收到此人的首份资料（直接抑制，身份 tombstone 照留）。

**续办（未完成时如何继续）**：进程在任意阶段边界被杀后——

```
rvault --store S withdraw status --withdrawal-id W   # 查 state 与 pending bundle
rvault --store S withdraw resume --withdrawal-id W   # 从断点继续
rvault --store S withdraw resume --all               # 续办所有未完成事项
```

`resume` 重入同一状态机：未清 edition 则清、重扫 bundle 登记、逐个补删。
相同 `withdrawal_id` + 相同 person 集合重试是幂等续做；**改集合即冲突**
（退出码 2）。两个撤回事项相交时均保持有效（屏障取并集，先到的删除，
后到的记 `missing`）。重试已完成事项是空操作，屏障与 tombstone 仍在，
**不会再次发出旧数据**。撤回状态查询只回显标识符与计数，不回显姓名或测量。

## 5. 并发设计

- 每 study 一把 **flock 文件锁**（`locks/`）；进程死亡由 OS 释放锁，
  占用必可在进程退出后恢复（演练中有 SIGKILL 持锁者后续办成功的用例）。
- bundle 生成：**读快照不加锁**，进入临界区（加锁）后、登记前**复查屏障**：
  - 撤回先登记 → bundle 中止（退出码 3），不留任何文件；
  - bundle 先登记 → 撤回的清理阶段通过登记表找到并删除它。
  两种先后关系都由同步点复现（见 §7），不靠 sleep 猜。
- intake 的屏障检查与写入在同一个 `BEGIN IMMEDIATE` 事务内，与撤回登记在
  数据库层串行，不会出现"检查通过后被撤回"的窗口。

## 6. 擦除范围与残留边界（重要）

**承诺擦除的**：store 内逻辑数据（`editions` 行、bundle 登记状态）与
`bundles/` 下的普通 bundle 文件。撤回完成后，被撤回人的
`display_name / measure / memo` 不存在于任何受管表、错误信息、回执或
bundle 文件中（演练 cp17/cp18 逐表逐文件扫描验证）。

**明确不在擦除承诺内的残留**：

- **SQLite 空闲页 / 旧 journal（或 WAL）页**：`DELETE` 释放的页在文件层面
  可能仍含旧字节，直到被复写。本程序以 `PRAGMA secure_delete=ON` 尽力清零，
  但这是存储引擎行为，不构成承诺；如需更强保证，请在擦除后离线执行
  `VACUUM` 并自行处理备份。
- **OS 页面缓存、文件系统/设备层副本**（写时复制、磨损均衡、快照）不可控。
- **原始来件副本**：发送方持有的 JSONL 原件、本程序之外的任何拷贝
  （包括 `demo/fixtures/`）不在管辖内。
- **日志与备份**：store 目录之外的任何归档。

即：承诺是**逻辑层 + 受管文件**的不可再读，不是物理介质级销毁。

## 7. 演练与独立校验

```
python3 demo/run_demo.py        # 每次重建 demo/work，全流程约 18 个阶段
```

演练覆盖：两个 study（alpha/beta）+ 并发用 study（conc/lockt/vandal）；
批次接收 → 修订替换（A1 三个 edition）→ 重叠参与者的多份 bundle →
撤回 W1（alpha 的 ann）→ 再导入（旧 edition、新 edition、新 observation、
旧 batch_tag 重送、含正常参加者的混合批次）→ 重叠撤回 W2={ann,cid} →
无历史先撤回 W0={zed} → 同 id 改名单被拒 → 屏障登记后强退（W4）、
清 edition 且删部分 bundle 后强退（W7）并续办 → 外部丢失 bundle 记
`missing` → 双进程复现"撤回落在 bundle 读取与登记之间"与"bundle 先登记"
两种先后 → SIGKILL 持锁进程后锁恢复 → 注册表路径被篡改时拒绝删除界外文件。

**同步点**（测试注入用，不影响正常运行）：设置 `RVAULT_SYNC_DIR` 后，程序在
`bundle.after_read`、`bundle.in_critical`、`bundle.after_register`、
`withdraw.after_register`、`withdraw.after_editions`、
`withdraw.after_bundle_delete.N`、`withdraw.before_complete` 等处写
`<名>.reached`；若该名列入 `RVAULT_SYNC_WAIT`（逗号分隔），则阻塞至
`<名>.proceed` 出现。演练驱动以此精确制造交错并在目标点 SIGKILL。

**独立校验**：`demo/expected.py` 是**手工计算**的期望答案（不经过生产实现）；
`demo/verify.py` 用自己的 SQL 直接读回 SQLite、逐字段比对 editions/投影/
屏障/撤回进度/bundle 登记表，并逐一读回 bundle 文件与回执文件比对内容，
最后做全库全文件残留扫描（被撤回人 payload 为零、未撤回人数据仍在作阳性
对照）。每个阶段结束即跑对应检查点，任一字段不符即失败。

## 8. 退出码

| 码 | 含义 |
|----|------|
| 0  | 成功（含 duplicate 幂等回执） |
| 2  | 输入被拒 / batch_tag 冲突 / 撤回集合冲突 |
| 3  | bundle 因撤回登记落在生成途中而中止 |
| 4  | 对象不存在或不可读（bundle 已删除、未知 id） |
| 5  | 完整性校验失败（bundle 摘要不符） |
