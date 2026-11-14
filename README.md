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

## 10. 修订级数据血缘（lineage）

研究数据处理作业把若干份原始观测合成新的观测。血缘子系统只**登记和查询**
"哪条输出修订用了哪些输入修订"的加工关系：不运行研究算法、不刷新索引
（不产生变更通知）、不生成数据包、不复制载荷、不改变观测数值。节点一律用
`(study, observation, edition)` 三元组精确定位，**绝不**绑定到"当前最高稿"。

```
rvault --store S lineage declare --job J --rule R [--input FILE]   # 登记一次加工作业
rvault --store S lineage leaf [--input FILE]                       # 声明原始采集叶子
rvault --store S lineage upstream   --node JSON [--max-depth N]    # 沿结果追到原始来件
rvault --store S lineage downstream --node JSON [--max-depth N]    # 从输入查出受影响的结果
rvault --store S lineage jobs                                      # 已登记作业清单
```

### 10.1 登记模型

- `declare` 的输入是 JSONL，每行一个输出：
  `{"output": {study,observation,edition}, "inputs": [{...}, ...]}`（inputs 非空）。
  一个作业可以有多个输出；同一输入可被多个输出使用；允许跨 study。
- `leaf` 的输入是 JSONL 节点列表，把修订显式声明为**原始采集叶子**。
- **未说明来源的修订属于"来源未知"**：没有父节点不等于原始数据，只有显式
  `leaf` 声明才算原始采集。原始叶子与加工输出互斥（同一修订不能两者皆是）。
- 声明只含标识符（三元组、作业标识、规则标识），不含姓名、测量或备注。

生效前的核对（任一不满足即整批拒绝，退出码 2）：

- 每个节点（输出与输入）都必须**确实存在对应修订且当前可读取**（在 editions
  中且未被撤回屏障挡住）；禁止为已撤回或不存在的修订补登记来源。
- 拒绝自依赖与**任意长度的环**：批量声明中的输出可以互相依赖，也可能与已登记
  关系一起构成环——一律对**合并后的关系**判断（BFS 求新输出的上游闭包后在
  子图上做拓扑删除），全部通过才保存。
- **一个输出的来源只能确定一次**：相同作业标识 + 规则标识 + 规范化后的完整
  关系（输入排序去重，顺序与重复输入不影响等价判断）重送返回 `duplicate`
  幂等成功；同标识改内容（或改规则标识）拒绝；另一作业试图重新认领已说明
  来源的输出也拒绝。
- 批量声明是**一个事务**：任何一个冲突都不会留下作业行、节点来源或部分边。
  声明在 `BEGIN IMMEDIATE` 内完成校验与写入，两个进程同时声明相反方向时，
  后到者在同一数据库写锁下重新检查，不会各自通过后合出环（演练 phase 6 用
  同步点复现）。

### 10.2 查询

输出为 JSONL 流：首行 `start`（起点身份、来源类型、可读状态），随后每个节点
一行，末行 `end` 给出记录数与 `complete: true` 结束标记。逐条输出，可管道。

- 每个返回节点给出**最短距离**（BFS 层序，真实边而非遍历遇到的第一条路径）
  和**一条见证路径**：每层按 (study, observation, edition) 三元组排序取最小
  前驱作为父指针，确定性可复算。共享祖先只出现一次。源节点本身不计入依赖
  列表。
- 上游节点区分：`leaf`（声明的原始叶子）、`unknown`（来源未知）、`derived`
  （有输入）。深度边界处仍有上游但未展开的节点标 `truncated: true`；下游边界
  处还有后续依赖的节点同样标记。`--max-depth 0` 只回起点状态，`end` 行的
  `depth_limited` 区分"确实没有"与"未展开"。
- 一次查询在**一个读事务**内完成：查询期间并发登记的新关系不会混进本次答复
  （演练 phase 7 用同步点复现）。
- 访问集合、距离与前沿放在 SQLite 临时工作表（磁盘页由引擎管理，内存有界），
  每个节点只访问一次，**不枚举路径**——深层大量共享祖先的图不会路径组合爆炸
  （演练 phase 9：600 节点 / 5900 边，理论路径 10^59，实际输出 590 条、
  峰值 RSS ≈ 35 MB）。

### 10.3 与撤回的关系

- 撤回**不删除、不改接**已有血缘关系；新到的更高 edition 不继承旧 edition 的
  边，未声明前显示来源未知。
- 查询把当前已撤回的节点标为 `"state": "withdrawn"`（edition 行已清除或人处于
  屏障下），身份关系仍可用来解释上下游；**不会**借追溯读回已撤回载荷（血缘表
  与查询输出里没有任何姓名/测量/备注）。
- 已撤回或不存在的修订不能补登记新来源（作为输出或输入都拒绝）。
- 这些只是血缘自身的可见性规则：不要求级联删除衍生结果，也不改现有撤回、
  通知或数据包协议。

### 10.4 手算小图与可复跑示例

`demo/run_lineage_demo.py` 每次在临时目录重建并逐条核对下图（含拒绝、并发、
规模与内存）；下面是同一组命令的手工推演。两个 study、一个多输出加工作业、
菱形共享祖先 + 较短替代路径、一个未知来源节点：

```
study alpha                         study beta
raw1 e1 (leaf)  raw2 e1 (leaf)      raw3 e1 (来源未知)
  |  \            |                   |
  |   \           |                   |
  |    \          v                   |
  |     `──────> out e1 [J1]          |
  |                |                  |
  |                v                  |
  `─────────────> mid e1 [J1] <───────+   (J1 是多输出作业：mid 与 out)
  |                |
  |                |
  +──────┐         |
         v         v
       final e1 [J2] ── 依赖 {mid e1, out e1, raw1 e1}

raw1 是菱形共享祖先（final→mid→raw1 与 final→out→raw1，深度 2），
另有较短替代路径 final→raw1（深度 1）→ raw1 只出现一次，距离 1。
```

```sh
S=/tmp/ln && PYTHONPATH=. python3 -m rvault --store $S init
# 导入两份来件（alpha: raw1,raw2,mid,final；beta: raw3,out），略去载荷字段
printf '%s\n' \
 '{"study":"alpha","person":"p1","observation":"raw1","edition":1,"display_name":"A","measure":10,"memo":""}' \
 '{"study":"alpha","person":"p2","observation":"raw2","edition":1,"display_name":"B","measure":20,"memo":""}' \
 '{"study":"alpha","person":"p3","observation":"mid","edition":1,"display_name":"C","measure":30,"memo":""}' \
 '{"study":"alpha","person":"p4","observation":"final","edition":1,"display_name":"D","measure":40,"memo":""}' \
 '{"study":"beta","person":"p5","observation":"raw3","edition":1,"display_name":"E","measure":50,"memo":""}' \
 '{"study":"beta","person":"p6","observation":"out","edition":1,"display_name":"F","measure":60,"memo":""}' \
 | PYTHONPATH=. python3 -m rvault --store $S import --batch-tag b1
# 原始叶子 + 两个加工作业
printf '%s\n' '{"study":"alpha","observation":"raw1","edition":1}' \
              '{"study":"alpha","observation":"raw2","edition":1}' \
 | PYTHONPATH=. python3 -m rvault --store $S lineage leaf
printf '%s\n' \
 '{"output":{"study":"alpha","observation":"mid","edition":1},"inputs":[{"study":"alpha","observation":"raw1","edition":1},{"study":"beta","observation":"raw3","edition":1}]}' \
 '{"output":{"study":"beta","observation":"out","edition":1},"inputs":[{"study":"alpha","observation":"raw1","edition":1},{"study":"alpha","observation":"raw2","edition":1}]}' \
 | PYTHONPATH=. python3 -m rvault --store $S lineage declare --job J1 --rule R1
printf '%s\n' \
 '{"output":{"study":"alpha","observation":"final","edition":1},"inputs":[{"study":"alpha","observation":"mid","edition":1},{"study":"beta","observation":"out","edition":1},{"study":"alpha","observation":"raw1","edition":1}]}' \
 | PYTHONPATH=. python3 -m rvault --store $S lineage declare --job J2 --rule R2
# 双向追溯
PYTHONPATH=. python3 -m rvault --store $S lineage upstream \
  --node '{"study":"alpha","observation":"final","edition":1}'
PYTHONPATH=. python3 -m rvault --store $S lineage downstream \
  --node '{"study":"beta","observation":"raw3","edition":1}' --max-depth 2
```

上游手算答案（final e1）：`mid e1`(1, derived)、`out e1`(1, derived)、
`raw1 e1`(1, leaf，走直接边)、`raw2 e1`(2, leaf)、`raw3 e1`(2, unknown)，
共 5 条；`--max-depth 1` 时 mid/out 标 `truncated: true`、raw1 不标。
下游手算答案（raw3 e1）：`mid e1`(1)、`final e1`(2)。

血缘表结构在 `rvault/db.py` 的 `SCHEMA` 里（`ln_*` 三张表），
`Store.ensure()` 在每条命令上执行 `CREATE TABLE IF NOT EXISTS`，旧 store
无需迁移即可使用。退出码沿用 §9：成功 0，校验/冲突/环拒绝 2（stderr 给出
JSON 错误码：`unknown_revision` / `revision_withdrawn` /
`output_already_claimed` / `leaf_conflicts_with_derived` / `job_conflict` /
`self_dependency` / `cycle_detected` / `invalid_declaration`）。

## 11. 版本化输入契约（contract）

上游单位对同一类资料使用的字段名、类型和版本并不一致。本子系统登记
"**上游承诺送什么形状的数据**"：按 `(提供方, 资料类型)` 维护一条版本化契约，
审查版本变更，并让导入在收件前明确选版本。它**不**负责撤回资料、通知索引、
登记加工血缘，也**不**自动改写来件——契约只做校验与可选的默认值补齐。

```
rvault --store S contract register --provider P --material-type M \
        --version N --fields FILE [--predecessor N] [--compatibility compatible|branch]
rvault --store S contract compare  --provider P --material-type M --against N --fields FILE
rvault --store S contract show     --provider P --material-type M --version N
rvault --store S contract list     [--provider P] [--material-type M]
rvault --store S import --batch-tag T [--input FILE] \
        [--provider P --material-type M --contract-version N]
```

### 11.1 字段定义

`--fields` 是 JSON：`{"fields": [ ... ]}`，每项：

| 键 | 含义 |
|---|---|
| `name` | 提供方送来的字段名，**非空字符串**，同一版本内唯一 |
| `type` | `string` / `integer` / `number` / `boolean` |
| `required` | 是否必需（`required` 与 `default` 不可同时出现） |
| `nullable` | 显式 `null` 是否合法 |
| `default` | 可选默认值；**必须非 null 且与 `type` 匹配** |
| `target` | 该字段映射到的规范导入字段（`study/person/observation/edition/display_name/measure/memo`）；契约里字段名可以和规范名完全不同，按 `target` 对应 |

- 提供方标识、资料类型、字段名都遵守非空字符串规则；`target` 不得重复。
- 一份用于导入的契约必须**恰好映射全部七个规范字段**。规范字段的类型由既有
  协议固定，唯一例外是 `measure`：可声明为 `integer` 或 `number`（兼容升级
  允许把整数拓宽为数值）。
- 映射到规范字段的字段必须要么 `required`，要么带 `default`；若 `nullable`
  则必须带 `default`（合法 `null` 要能落到一个具体存储值）。契约**不改写**
  已投递的数值——默认值只用于补齐"没送"或"合法 null"的字段。

### 11.2 发布与不可改写

- 每个 `(提供方, 资料类型, 版本)` **一经发布不可改写**。完全相同内容的
  重送返回 `status: "duplicate"`（幂等确认，不新增行）；同版本不同内容返回
  退出码 2 与 `content_conflict`。
- 首版必须是版本 1、无前版、声明为 `initial`。后续版本必须恰好等于
  `max + 1`（跳号/复用 ⇒ `version_gap`），必须用 `--predecessor` 指明
  **同一 (提供方, 资料类型) 下已存在**的前版（缺失 ⇒ `predecessor_required`
  / `unknown_predecessor`）。
- 前版指针图会从 `predecessor` 逐级回走并带访问集合；自指或形成环 ⇒
  `cycle_detected`。所有校验与写入在**同一个 `BEGIN IMMEDIATE` 事务**内完成，
  被拒的注册不留半条版本记录。
- 并发注册同一后继版本时，`BEGIN IMMEDIATE` 写锁 + 主键 `(provider,
  material_type, version)` 保证后来者只能看到一个已提交的版本：要么成为
  同一版本（内容相同则幂等 `duplicate`、不同则 `content_conflict`），要么
  在编号规则前被拒；不会出现两条版本记录或半条记录（见 §11.6 阶段 8）。

### 11.3 兼容声明与变更清单

每次注册都会与**指定的前版**逐字段比较，生成确定性的变更清单（按
`(字段, 变更种类)` 排序），随版本一起保存，可经 `show` 复查；`compare` 用
同一算法在发布前独立复算。变更种类：

| 种类 | `compatible` | 说明 |
|---|---|---|
| `field_defined` / `field_added` | 新增可选且带非空默认值的字段 ⇒ true；增加必需字段 ⇒ false |
| `field_removed` | false |
| `nullable_relaxed` | true（放宽空值限制） |
| `nullable_tightened` | false（收紧空值） |
| `type_widened` | true（`integer → number`） |
| `type_narrowed` / `type_changed` | false |
| `required_changed` | false |
| `default_added` / `default_removed` / `default_changed` | false |
| `target_changed` | false（改变字段含义） |

- `--compatibility compatible`（兼容升级）**只允许**上表标 true 的变更；
  只要清单里有一个 false 项就整次拒绝（`incompatible_change`），并在错误
  载荷里附完整变更清单——**不能伪装成兼容升级**。
- `--compatibility branch`（新分支）可以包含不兼容变更，但仍须引用存在的
  前版，且存储的兼容声明如实记为 `branch`。`compare` 输出的
  `classification` 为 `compatible` / `branch_required` / `no_change`。
- 首版省略 `--compatibility`，或显式写 `initial` 均可。

### 11.4 带契约的导入

`import` 同时给出 `--provider/--material-type/--contract-version` 时：

- 按**指定的这一版**逐行检查，绝不悄悄改用最新版；版本不存在 ⇒ 整批拒绝，
  退出码 **4**，错误码 `unknown_contract_version`（并给出该契约线的
  `latest`）。
- 逐行拒绝类别（诊断含 `line`、`field`、`reason`，**不含** display_name /
  measure / memo 的值）：`unknown_field`、`missing_required_field`、
  `type_mismatch`、`null_not_allowed`、`value_out_of_range`（规范字段域规则，
  例如 `edition` 必须为正整数）、`line_not_object`；契约里存储的默认值若
  不符合自身约束，该行报 `invalid_default`（不会写入域外值）。
- 任一行不合法 ⇒ **整批**不进入观测库：不写 editions、不写 batches、
  不生成回执文件、不产生任何变更通知；诊断逐处给出批次行号、字段名和类别。
- 合法行的可选字段按契约默认值补齐（显式 `null` 且 `nullable` 时落默认值），
  然后交给**既有资料接收逻辑**（同一 `_apply_obj` 路径）。因此：同一业务
  值按原规则保留，`accepted` / `accepted_existing` / `withdrawn_suppressed`
  / 归属冲突 / edition 冲突 / 幂等 batch_tag / 通知语义全部与旧协议一致。
- 批次凭据与协议/版本一起记入 `ct_batches`（同一事务）。用过的 batch_tag
  若改协议或改契约版本重送，即使字节相同也拒绝，凭据不会悄悄切换协议。

**旧命令不变**：不带任何契约选择器时，`import` 继续走原来的固定字段协议，
行为、回执、通知逐字节一致；`ct_batches` 记 `protocol='legacy'`。

### 11.5 手算小例子

两份提供方、不同字段名与类型，再各加一个兼容版本和一条新分支：

```sh
S=/tmp/ct && PYTHONPATH=. python3 -m rvault --store $S init
# 提供方 labA，资料类型 obs：字段名与规范名不同
cat > /tmp/labA_v1.json <<'EOF'
{"fields":[
 {"name":"study","type":"string","required":true,"nullable":false,"target":"study"},
 {"name":"person","type":"string","required":true,"nullable":false,"target":"person"},
 {"name":"obs_id","type":"string","required":true,"nullable":false,"target":"observation"},
 {"name":"rev","type":"integer","required":true,"nullable":false,"target":"edition"},
 {"name":"label","type":"string","required":true,"nullable":false,"target":"display_name"},
 {"name":"value","type":"integer","required":true,"nullable":false,"target":"measure"},
 {"name":"note","type":"string","required":false,"nullable":false,"default":"","target":"memo"}]}
EOF
cat > /tmp/labB_v1.json <<'EOF'
{"fields":[
 {"name":"study","type":"string","required":true,"nullable":false,"target":"study"},
 {"name":"subject","type":"string","required":true,"nullable":false,"target":"person"},
 {"name":"sample","type":"string","required":true,"nullable":false,"target":"observation"},
 {"name":"attempt","type":"integer","required":true,"nullable":false,"target":"edition"},
 {"name":"title","type":"string","required":true,"nullable":false,"target":"display_name"},
 {"name":"reading","type":"integer","required":true,"nullable":false,"target":"measure"},
 {"name":"remark","type":"string","required":false,"nullable":false,"default":"","target":"memo"},
 {"name":"verified","type":"boolean","required":false,"nullable":false,"default":false},
 {"name":"batch","type":"integer","required":false,"nullable":false,"default":0}]}
EOF
PYTHONPATH=. python3 -m rvault --store $S contract register --provider labA --material-type obs --version 1 --fields /tmp/labA_v1.json
PYTHONPATH=. python3 -m rvault --store $S contract register --provider labB --material-type measurement --version 1 --fields /tmp/labB_v1.json
# 兼容升级：加可选 site（默认 "unknown"）、放宽 note 空值、把 value 拓宽为 number
cat > /tmp/labA_v2.json <<'EOF'
{"fields":[
 {"name":"study","type":"string","required":true,"nullable":false,"target":"study"},
 {"name":"person","type":"string","required":true,"nullable":false,"target":"person"},
 {"name":"obs_id","type":"string","required":true,"nullable":false,"target":"observation"},
 {"name":"rev","type":"integer","required":true,"nullable":false,"target":"edition"},
 {"name":"label","type":"string","required":true,"nullable":false,"target":"display_name"},
 {"name":"value","type":"number","required":true,"nullable":false,"target":"measure"},
 {"name":"note","type":"string","required":false,"nullable":true,"default":"","target":"memo"},
 {"name":"site","type":"string","required":false,"nullable":false,"default":"unknown"}]}
EOF
PYTHONPATH=. python3 -m rvault --store $S contract compare  --provider labA --material-type obs --against 1 --fields /tmp/labA_v2.json
PYTHONPATH=. python3 -m rvault --store $S contract register --provider labA --material-type obs --version 2 --predecessor 1 --compatibility compatible --fields /tmp/labA_v2.json
# 新分支：删 note、加必需 flag —— 必须显式 branch
cat > /tmp/labA_v3.json <<'EOF'
{"fields":[
 {"name":"study","type":"string","required":true,"nullable":false,"target":"study"},
 {"name":"person","type":"string","required":true,"nullable":false,"target":"person"},
 {"name":"obs_id","type":"string","required":true,"nullable":false,"target":"observation"},
 {"name":"rev","type":"integer","required":true,"nullable":false,"target":"edition"},
 {"name":"label","type":"string","required":true,"nullable":false,"target":"display_name"},
 {"name":"value","type":"number","required":true,"nullable":false,"target":"measure"},
 {"name":"flag","type":"string","required":true,"nullable":false,"target":"memo"},
 {"name":"site","type":"string","required":false,"nullable":false,"default":"unknown"}]}
EOF
PYTHONPATH=. python3 -m rvault --store $S contract register --provider labA --material-type obs --version 3 --predecessor 2 --compatibility branch --fields /tmp/labA_v3.json
# 带契约导入：note 省略→补默认；value 可小数
printf '%s\n' \
 '{"study":"s1","person":"p1","obs_id":"o1","rev":1,"label":"N","value":10}' \
 '{"study":"s1","person":"p1","obs_id":"o2","rev":1,"label":"N","value":10.5,"note":null}' \
 | PYTHONPATH=. python3 -m rvault --store $S import --batch-tag b1 \
     --provider labA --material-type obs --contract-version 2
PYTHONPATH=. python3 -m rvault --store $S query --study s1
```

手算答案：v2 相对 v1 的变更清单恰为三项——`note nullable_relaxed`、
`site field_added`、`value type_widened`，`classification=compatible`；
v3 相对 v2 为 `flag field_added`（false）与 `note field_removed`（false），
`classification=branch_required`，以 `compatible` 提交会被拒。导入后 `o1`
的 `memo` 为 `""`（默认补齐）、`o2` 的 `memo` 为 `""`（显式 null 落默认）、
`o2` 的 `measure` 为 `10.5`（数值原样保留）。

### 11.6 演练与独立校验

```
python3 demo/run_contract_demo.py        # 10 个阶段，全部工件在临时目录
```

`demo/expected_contracts.py` 是**手工计算**的期望（不经过实现）；
`demo/verify_contracts.py` 用**自己的 SQL** 直接读回 `ct_contracts` /
`ct_fields` / `editions` / `ct_batches` / `cf_events`，逐字段与字面量比对。
`demo/contract_specs.py` 只是**测试输入**，演练启动时把它们写进临时目录。

阶段与断言：

| 阶段 | 内容 |
|---|---|
| 1 | 两个提供方各登记初版：字段名与规范名不同（labA 用 `obs_id/rev/label/value`），类型也不同（labB 含 boolean 与可选 integer 字段）；同内容重送幂等 |
| 2 | `compare` 复算 v2 对 v1 的逐字段清单，再以 `compatible` 发布；labB v2 只拓宽 `batch` |
| 3 | v3 对 v2 为 `branch_required`；以 `compatible` 提交被拒（不能伪装），再以 `branch` 发布 |
| 4 | 拒绝清单：删字段 / 收紧空值 / 窄化类型 / 增必需字段 / 改含义 / 改必需状态 / 改默认值 / 空默认值 / 空字段名；跳号、缺前版、自指成环、缺前版声明、内容不符重送；另用损坏的私有库复现"前版指针图成环"（正常 CLI 的 max+1 编号无法构造长环，此路径为防御性校验）；每次拒绝后重开库确认版本图未变 |
| 5 | 带契约导入：可选默认值补齐、显式 null 落默认、小数 measure 原样存储；labB 的 boolean/integer 默认值 |
| 6 | 重开数据库后结果不变（新进程读库） |
| 7 | 一批前 3 行合法、末行非法：整批拒收、无 editions/batches/回执/通知残留；诊断只含行号/字段名/类别，**不含**样本载荷；各类拒绝类别逐一核对；v3 形状的行在 v2 下被拒、显式 v3 才收；未知版本拒绝 |
| 8 | 两个进程竞争同一后继版本：一个完整版本胜出，败者 `content_conflict`，字段行数只属于完整版本 |
| 9 | 原有**无契约**导入照旧（固定字段、整数 measure、回执无契约字段），变更通知照旧 |
| 10 | 撤回与通知仍可用：契约数据被撤回擦除、契约元数据不动、remove 后不再 refresh、通知不含载荷 |
