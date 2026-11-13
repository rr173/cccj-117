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
locks/          每 study 一个 flock 锁文件（变更通知使用 study "*" 的锁）
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
  在事务中应用，回执流式写入文件。实测 25 MB / 20 万行输入峰值 RSS 约 17 MB
  （含变更通知的按对象去重，去重状态在 SQLite 暂存表里按磁盘页增长；
  事务提交后暂存表清空）。

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

## 5. 变更通知出口（feed）——给下游检索索引

下游索引不必再反复扫描整个 store：程序在**业务事务内部**写出持久通知，每个
索引作业以**自己的名称和确认位置**消费。通知只表达"这个对象需要重新读取"
或"这个对象已被撤回、不得再读"，**不携带** display_name / measure / memo，也
不提供历史载荷下载；consumer 拿通知后仍走原有读路径（`query` / `history` /
`bundle read`）重新读取。通知表里没有任何研究载荷副本，也没有全量快照服务。

```
rvault --store S feed register --consumer NAME [--from-seq N] [--reactivate]
rvault --store S feed pull     --consumer NAME [--limit N] [--preview]
rvault --store S feed ack      --consumer NAME --token TOKEN
rvault --store S feed disable  --consumer NAME
rvault --store S feed status   [--consumer NAME]      # 积压/位置/保留下界
rvault --store S feed trim     [--through-seq N]
```

### 5.1 事件从哪来（与业务写入同事务提交）

| 业务操作 | 通知 |
|---|---|
| intake 新 observation，或**新存储了最高 edition** | 该对象 `refresh` |
| intake 重复投递同一 edition（内容相同，`accepted_existing`） | 无 |
| intake 收入**低于当前最高的合法旧 edition** | 无 |
| intake 中的人处于撤回屏障（`withdrawn_suppressed`） | 无（不产生 refresh） |
| 批次被拒（坏行/归属冲突/edition 冲突） | 整批回滚，**无任何可消费事件** |
| `withdraw register` 的屏障行提交 | 该人**本次新认领**的每个 observation `remove` |

- 通知行由**同一个 SQLite 事务**写入：批次 COMMIT 才可消费，ROLLBACK 则连通知
  一起消失；撤回的 remove 与屏障行同一事务提交，因此**屏障一生效 remove 即可
  消费**，不必等 edition 清除和 bundle 文件删除结束。既不会"先确认业务成功再
  补写通知"，也不会出现"业务失败但留下孤立通知"。
- 撤回删除的是 edition 行；`observation_owner` 作为身份墓碑保留，remove 集合
  来自它与本次新写入的 `barrier` 行（`withdrawal_id` 匹配）。因此重叠撤回
  （W2={ann,bob} 而 ann 已被 W1 认领）只为**新认领的 bob** 产生 remove，不会
  重复通知 ann 的对象。

### 5.2 事件字段与稳定排序

`cf_events` 一条通知 = `(seq, frame_id, ordinal, op, study, observation)`：

- `seq`：全局递增整数，在业务事务内分配；
- `frame_id`：一次业务操作（一次提交）一个连续帧号；`frame_ts` 取该帧首事件
  时间，因此 `(frame_ts, seq)` 与帧号顺序一致；
- `ordinal`：帧内序号；
- `op`：`refresh` | `remove`；
- 对象标识：`study` + `observation`（不含 person/姓名/测量/备注）。

**帧内**按 `(study, observation)` 确定性排序并按对象去重：同一对象在一次操作
中多次修订只产生一条；同一对象 `remove` 覆盖 `refresh`。**帧间**按 `seq` 升序。
已被 remove 的对象以后**永不再产生 refresh 或重复 remove**（`cf_removed_objects`
持久记录，收集与落库时都检查）。

### 5.3 消费者与生命周期

- 新登记默认从**当下末尾**开始（`start_seq = tail+1`），只收未来变动；旧事件
  不会自动补发。
- 也可用 `--from-seq N` 明确要求一个**仍被保留**的位置。位置已低于保留下界时
  **登记失败**（退出码 6），并报告 `requested_seq / retention_floor /
  earliest_available_seq`——绝不悄悄从较新处继续。
- 登记即启用（可接收）；`disable` 解除该名称的保留责任、作废其未确认投递凭据。
- 再次使用同名消费者视为**新生命周期**：必须 `--reactivate` 且**必须显式给出
  起点**（不能默认"从当下"），generation +1，旧凭据一律失效。
- 两个名称各有独立 `acked_seq`，互不推进。

### 5.4 投递协议

`feed pull` 每次最多物化**一份**待确认投递（先落库 `cf_deliveries` /
`cf_delivery_frames`，后输出），逐行协议：

```
START delivery=<token> from=<s> through=<e> target=<n> frames=<k> head=<h> events=<c> empty=0 replay=<0|1>
FRAME id=<fid> index=<i> of=<k> ts=<iso> start=<s> end=<e> events=<n>
EVENT seq=<n> frame=<fid> ordinal=<i> op=<refresh|remove> study=<s> observation=<o>
ENDFRAME id=<fid> events=<n> first=<s> last=<e>
END events=<n> frames=<k> empty=0
```

- `--limit N`（默认 1000）是**目标条数**，不是硬拆分：帧不可拆分，达到目标后在
  **帧边界**停下；**首个帧本身超过目标时整帧发送**，START/FRAME/END 中的
  `events` 给出实际条数（演练中 `--limit 3` 收到 4 个事件的首帧即此情形）。
- 只要存在未确认投递，**再次 pull 必然重放同一范围与同一 token**——期间有新通知
  写入也一样；输出写到一半进程被杀也一样（范围先落库）。
- 无待发事件时返回可识别的空结果 `START empty=1 …` / `END events=0 frames=0
  empty=1`，不创建无法确认的幽灵投递。
- 逐帧用 SQLite 游标逐条发送，**不会先把大帧拼成数组**（实测 12 万事件的单帧，
  发送端峰值 RSS ≈ 17 MB，与处理普通帧时相同）。
- 消费者可据 `FRAME events`、`ENDFRAME`、`END` 的计数识别截断（缺 ENDFRAME/END
  即被截断）。

```
rvault --store S feed ack --consumer NAME --token TOKEN
```

显式确认才前移位置，且只前移到该投递的 `through`。重复确认同一 token 幂等
（返回 `replayed: true`，位置不变）；来自其它消费者、旧生命周期、已停用名称或
并非"当前待确认"的 token 一律拒绝（退出码 2），不跳过任何资料。

多进程同时 pull / ack 同一名称时，由 store 级 flock（`locks/` 中 study `"*"`）
串行化投递物化与确认；同名并发 pull 只会得到**一份**待确认范围与一个 token，
不会互相覆盖。

### 5.5 保留裁剪（trim）

- `feed trim` 只删除**所有启用消费者都已确认的完整帧**。任一慢消费者未确认即
  挡住裁剪（`blocked_by` 列出挡路者）。帧从不部分删除。
- **没有启用消费者**时，无上界的 trim 被拒绝（退出码 2）；操作者必须给出
  `--through-seq N` 明确上界。停用的消费者不再承担保留责任。
- 保留下界持久记录（`cf_meta.retention_floor`）；请求已被裁掉的位置时报告缺口
  与最早可用位置（见 5.3），不会静默跳到较新位置。
- 裁剪与登记在**同一把锁**下互斥，且都在锁内复查下界，因此并发时结果只可能是
  二者之一：登记被拒（附下界），或登记成功且承诺的事件仍在保留范围内
  （演练 phase 12 用同步点复现两种先后）。

### 5.6 安装

- 表结构在 `rvault/db.py` 的 `SCHEMA` 里（`cf_*` 七张表），`Store.ensure()` 在
  每条命令上执行 `CREATE TABLE IF NOT EXISTS`，因此**升级现有 store 无需单独迁移
  步骤**，直接运行任意命令（如 `feed status`）即可建表。
- **安装时不补造历史**：已有资料不产生任何 `refresh`/`remove`，事件从安装后的
  第一次业务写入开始。新登记的消费者默认从当下开始，不会读到安装前的数据；
  如确需从头消费，只能消费**安装之后**仍被保留的事件。
- 通知表只存标识符与位置，不含载荷，所以不会扩大 §7 的擦除范围。`cf_intents`
  是事务内的暂存区，随事务提交/回滚一起清空。

## 6. 并发设计

- 每 study 一把 **flock 文件锁**（`locks/`）；进程死亡由 OS 释放锁，
  占用必可在进程退出后恢复（演练中有 SIGKILL 持锁者后续办成功的用例）。
- bundle 生成：**读快照不加锁**，进入临界区（加锁）后、登记前**复查屏障**：
  - 撤回先登记 → bundle 中止（退出码 3），不留任何文件；
  - bundle 先登记 → 撤回的清理阶段通过登记表找到并删除它。
  两种先后关系都由同步点复现（见 §8），不靠 sleep 猜。
- intake 的屏障检查与写入在同一个 `BEGIN IMMEDIATE` 事务内，与撤回登记在
  数据库层串行，不会出现"检查通过后被撤回"的窗口。

## 7. 擦除范围与残留边界（重要）

**承诺擦除的**：store 内逻辑数据（`editions` 行、bundle 登记状态）与
`bundles/` 下的普通 bundle 文件。撤回完成后，被撤回人的
`display_name / measure / memo` 不存在于任何受管表、错误信息、回执或
bundle 文件中（演练 cp17/cp18 逐表逐文件扫描验证；通知表逐列扫描亦不含载荷）。

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

## 8. 演练与独立校验

```
python3 demo/run_demo.py         # 每次重建 demo/work，全流程约 18 个阶段
python3 demo/run_feed_demo.py    # 变更通知出口：15 个阶段，重建 demo/work_feed
python3 demo/verify_feed_txn.py  # 生产者事务回滚 / 并发生产者（也可由上面的演练调用）
```

### 8.1 主流程演练（run_demo.py）

演练覆盖：两个 study（alpha/beta）+ 并发用 study（conc/lockt/vandal）；
批次接收 → 修订替换（A1 三个 edition）→ 重叠参与者的多份 bundle →
撤回 W1（alpha 的 ann）→ 再导入（旧 edition、新 edition、新 observation、
旧 batch_tag 重送、含正常参与者的混合批次）→ 重叠撤回 W2={ann,cid} →
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

### 8.2 变更通知演练（run_feed_demo.py）

两个**真实消费进程**（`live` 及时确认、`slow` 滞后）走完修订、撤回与保留裁剪：

- 源端每次写入后用既有 `query` 命令核对**实际对象变化**；两边收到的每份投递
  都逐行核对帧号、seq、ordinal、op、对象与确认位置；
- 期望通知清单位于 `demo/expected_feed.py`（手写，不经实现）；数据库由
  `demo/verify_feed.py` 用自己的 SQL 读回核对；`demo/verify_feed_txn.py`
  另外验证生产者事务回滚与并发提交。

阶段与断言：

| 阶段 | 内容 |
|---|---|
| 1-3 | 安装即用；首个批次一帧四对象；帧超目标仍整帧；未确认时重放同 token/同范围（**期间新写入也不改变范围**，head 只体现在计数上）；两次修订只产生一条；**旧稿补收、内容未变的重复来件不通知** |
| 4 | 坏批次被拒后**没有孤立通知**；紧接着的合法写入恰好 +1 事件 |
| 5-7 | 次级消费者按显式保留位置登记；**屏障提交时 remove 即可消费**（撤回进程停在 `withdraw.after_register`，edition 与文件尚未清除）；撤回后 intake 被抑制的行不通知 |
| 8 | 重复撤回不重复通知；remove 覆盖 refresh；被撤回对象不再被 refresh 带回 |
| 9 | 慢消费者挡住裁剪；逐帧推进的保留下界（0→4→6→9→13→16→17）与每步裁剪量 |
| 10 | 过期起点返回缺口诊断（请求位置 / 下界 / 最早可用位置），登记原子失败 |
| 11 | 投递输出中途 SIGKILL 后重放同 token；确认后 SIGKILL 再确认（新进程读库，不依赖进程内状态）；同名并发拉取只得一份待确认范围；不同消费者/未知名称的凭据被拒；停用撤销凭据、重建后旧凭据失效 |
| 12 | 裁剪与新登记两种交错顺序（同步点 `feed.trim.after_commit` / `feed.register.before_commit`）；无启用消费者时无上界裁剪被拒、显式上界可用；上界之后的位置缺口诊断 |
| 13 | **12 万事件单帧**：目标 100 仍整帧发送，首尾计数核对；发送端实测峰值 RSS ≈ 17 MB（逐条游标流式，非全帧常驻） |
| 14 | 生产者停在 COMMIT 之前：业务行与通知行对其它连接都不可见；COMMIT 后整帧同时可消费 |
| 15 | 旧 store 安装后**不补造历史**，只通知安装后的写入 |

**通知侧同步点**（独立命名空间，设置 `RVAULT_CF_SYNC_DIR`；
`RVAULT_CF_SYNC_WAIT` 中的名字会阻塞到 `<名>.proceed`）：
`feed.intake.before_line`、`feed.commit.finalize`、`feed.register.before_commit`、
`feed.delivery.event.N`、`feed.delivery.during_frame`、`feed.after_ack`、
`feed.after_disable`、`feed.trim.before_delete`、`feed.trim.after_commit`。

## 9. 退出码

| 码 | 含义 |
|----|------|
| 0  | 成功（含 duplicate 幂等回执） |
| 2  | 输入被拒 / batch_tag 冲突 / 撤回集合冲突 / 通知确认被拒 / 裁剪被拒 |
| 3  | bundle 因撤回登记落在生成途中而中止 |
| 4  | 对象不存在或不可读（bundle 已删除、未知 id） |
| 5  | 完整性校验失败（bundle 摘要不符） |
| 6  | 通知起点已不可用（低于保留下界；报文给出缺口与最早可用位置） |
