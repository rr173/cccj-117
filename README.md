# rvault — 本地研究资料接收与撤回处理程序

落实"参与者撤回授权后，从后续研究数据中消失"的本地工具：接收逐行 JSONL
批次、保存全部 edition 历史、为分析人员生成数据包（bundle），并以可分阶段、
可崩溃续办的方式执行撤回擦除。范围仅限工作区逻辑数据与普通 bundle 文件；
**不包含**用户账号、审批流等周边系统。

除 §13 的嵌套整备子系统需要 **ijson** 库（`pip install ijson`）外，其余功能
无第三方依赖，Python ≥ 3.9 标准库即可运行；不安装 ijson 时旧命令照常工作，
只有 `nested` 子命令会提示缺少依赖。

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

## 12. 离线邮件来件通道（mail）

数据接收组收到的是上游导出的原始邮件文件。本子系统把 `.eml` 文件作为
**离线**来件通道：读取原始 MIME 字节流，找到清单真正指向的附件，完整
解码后交给**既有批次接收入口**（`import` 的同一 `import_batch` 路径）。
它不连接邮箱、不发信、不提供邮件界面，也**不另开一套**绕过批次凭据、
撤回屏障或通知语义的写库方法。

```
rvault --store S mail receive --input FILE.eml [--chunk-size N]
rvault --store S mail resume  --input FILE.eml [--chunk-size N]
rvault --store S mail status  [--message DIGEST]
```

### 12.1 邮件约定

- **清单**是 Content-Type 为 `application/vnd.rvault-batches+json` 的单独
  叶子部分，整封邮件**恰好一份**。内容为
  `{"batches": [{"batch_tag": ..., "content_id": ..., "sha256": ...}, ...]}`；
  `sha256` 针对**传输解码之后**的原始附件字节（64 位小写十六进制）。
- **数据附件**的 Content-Type 为 `application/x-ndjson`，解码后的内容遵守
  原有七字段观测协议（与 `import` 的旧协议逐字节一致）。
- 清单与附件只靠 **Content-ID** 关联——绝不靠文件名或附件排列顺序。
- 容许嵌套的 `multipart/mixed` 与 `multipart/related`；未被清单引用的
  说明文字和图片**忽略**；正文里的 JSON 示例只是说明文字，不是清单；
  `message/rfc822` 部分视为不透明载荷，**不递归**捡出另一封信的批次。
- 传输编码范围：`base64`、`quoted-printable`、`7bit`、`8bit`、`binary`。
  解码**字节保真**：JSONL 的 CRLF、非 ASCII 文本、文件末尾有无换行都
  原样保留；邮件从不被先统一当文本重写。折行 base64 与 QP 软换行按规则
  消解；分隔线可能被读块拆开、正文里与边界同前缀的行**不是**分隔线。
- **文件名只作元数据**：像 `../../data.jsonl` 这样的名字不得决定宿主
  写入路径（解码产物一律落在接收器自己的 `tmp/` 里，由系统命名）。

### 12.2 两阶段与崩溃续办

1. **预检（整封邮件，先于任何业务接收）**：流式解析结构、逐附件解码到
   受管临时区并边读边算摘要，然后判明——清单缺失/重复/损坏、Content-ID
   重复、清单引用不到附件或指向非数据部分、同一 batch_tag 指向两份载荷、
   编码非法/不支持、multipart 未闭合、摘要不符。**任一问题即整封拒绝，
   一个批次都不会入库**——最后一个附件损坏时，前面的好附件也尚未入库。
2. **导入**：按清单顺序逐附件调用既有 `import_batch`（每个附件一个独立
   的既有批次事务，**不**把多份附件合成一个事务）。某份附件观测行不合法
   时**这一批**整批拒绝，之前已成功的批次保留，后面的批次继续；总结果
   逐批列出 `accepted` / `duplicate` / `rejected`，邮件整体结果
   （`received` / `partial` / `rejected`）不会掩盖一份被拒的批次。

**持久处理记录**以原始邮件字节摘要为身份（`ml_messages` / `ml_batches`
表，随 `Store.ensure()` 自动建表），关联清单位置、真实 batch_tag 与接收
结果。相同邮件重新处理是**续办**：已终结的批次不再重导，待办批次继续；
不会新增观测、修订或通知。若进程在**业务批次已提交、处理记录尚未更新**
处被强退，续办时靠原 batch_tag 凭据重新确认（intake 返回 `duplicate`），
这一批不会被再次当新数据计入。`mail resume` 与 `mail receive` 走同一
管线，只是要求邮件已有处理记录（否则退出码 4）。

### 12.3 临时区、内存与诊断

- 解码附件只落在 store 的 `tmp/`（`mail.<pid>.<n>.part`）；正常结束与
  已判定的拒绝都会清理；意外退出留下的文件由下次运行的 `reconcile`
  按属主 pid 回收——临时文件**永不**作为已接收凭据。
- 邮件可大于进程可用内存：解析、解码、摘要全部流式进行，不整封读入、
  不组装内存 MIME 对象树。实测 19.7 MB 邮件（12 万行观测）接收峰值
  RSS 约 20 MB。
- 诊断只展示邮件摘要、部分位置、批次号与错误类别，**不**复制正文、
  姓名、测量值或备注。部分编号：邮件本身为 `0`，顶层部分为 `1,2,...`，
  嵌套部分点分（`2.1`）。

预检错误类别：`manifest_missing` / `manifest_duplicate` /
`manifest_invalid` / `manifest_too_large` / `content_id_duplicate` /
`reference_unresolved` / `reference_ambiguous` /
`referenced_part_not_data` / `batch_tag_duplicate` / `digest_mismatch` /
`unsupported_transfer_encoding` / `invalid_transfer_encoding` /
`unterminated_multipart` / `invalid_multipart_boundary` /
`headers_too_large` / `structure_too_deep` / `too_many_parts` /
`message_changed`。

退出码沿用 §9：全部批次被接收（含幂等 duplicate）为 0；预检拒绝或任一
批次被拒为 2；邮件不可读或 `resume`/`status` 遇到未知邮件摘要为 4。

### 12.4 演练与独立校验

```
python3 demo/run_mail_demo.py      # 全部工件在临时目录，启动时打印路径
```

演练**直接生成真实原始 MIME 字节**（不用事先拆好的附件假装邮件）：
`demo/expected_mail.py` 是手工期望值（载荷字面量、由字面量算出的摘要、
期望库内元组与通知序列）；`demo/verify_mail.py` 用自己的 SQL 直接读回
`editions` / `batches` / `cf_events` / `ml_messages` / `ml_batches` 逐字段
比对。逐条复跑说明与预先列好的期望结果见 `demo/MAIL_DEMO.md`。

| 阶段 | 内容 |
|---|---|
| 1 | 嵌套邮件：说明文字（内含 JSON 示例与边界相似行）+ multipart/related 内嵌图片 + base64 附件（CRLF、非 ASCII、末尾无换行）+ QP 附件（软换行、LF、末尾有换行）+ 清单；核对解码字节摘要、真实库内观测与通知数量（2 帧 4 事件）、临时区清理 |
| 2 | 同一邮件以 **7 字节读块**接收（分隔线、base64 四元组、QP 软换行分别跨块），与正常块大小的批次摘要、editions、通知、处理记录逐项相同 |
| 3 | 十种预检拒绝：摘要不符、Content-ID 歧义、缺结束边界、声明不支持的编码、非法 base64/QP、message/rfc822 不递归、清单重复、清单损坏、同批次号两份载荷、引用不到附件；诊断不含载荷值；editions 为空 |
| 4 | 越界文件名 `../../data.jsonl`：正常接收，宿主文件系统不出现该名字的文件 |
| 5 | "首批合法、次批行错、末批合法"：部分接收回执 accepted/rejected/accepted，库内只有第 1、3 批；重放同一邮件只回读记录 |
| 6 | 一批已落库但办理记录未更新处 SIGKILL；续办后该批经原凭据确认为 duplicate，无重复数据或通知，后续附件完成；崩溃遗留临时文件被回收 |
| 7 | 邮件通道走既有撤回屏障：屏障内参与者的行被抑制（`withdrawn_suppressed`）且不产生通知，其余行正常入列 |
| 8 | 大附件邮件（19.7 MB / 12 万行）：报告输入规模与实测峰值内存（约 20 MB） |
| 9 | `mail status` 单封/列表/未知摘要（退出码 4）；`resume` 未知邮件（退出码 4） |


## 13. 嵌套研究表整备（nested）

有一类来件不是一行一个观测，而是一个参加者一条根记录、每次访视各自带读
数数组、参加者另有独立授权条目。§1 的七字段接收与 §12 的邮件入口都不处理
这种嵌套结构。本子系统是**独立模块**：读一份顶层为数组的 JSON 文件，按一份
**声明式整备方案**投影出**自己的关系数据库**（每张方案表一张物理表），不改
造观测协议、不写 `vault.sqlite3` 的业务表、不发变更通知、不计算研究指标。

```
rvault --store S nested plan show --plan FILE
rvault --store S nested prepare --plan FILE --input FILE --database DB
rvault --store S nested resume  --plan FILE --input FILE --database DB
rvault --store S nested status  --database DB [--source DIGEST]
rvault --store S nested roots   --database DB [--source DIGEST] [--root N]
rvault --store S nested read    --database DB --table T [--source DIGEST]
                  [--root N] [--limit N]
rvault --store S nested children --database DB --table T --parent ROW_ID [--limit N]
```

`--database` 是**产物库**（自建，含投影表与本次整备的运行记录）；`--store`
仍是受管 store，只用于 scratch 暂存与既有命令。`prepare` 与 `resume` 走同
一条管线（`resume` 要求该源摘要已有运行记录，否则退出码 4）。

### 13.1 方案格式

方案是 JSON 对象，**恰好**三类键：

```json
{
  "version": 1,
  "root": {
    "name": "participants",
    "columns": [
      {"name": "person", "path": ["person"], "type": "string"},
      {"name": "study", "path": ["study"], "type": "string"}
    ]
  },
  "tables": [
    {"name": "visits", "parent": "participants", "path": ["visits"],
     "columns": [
       {"name": "visit_label", "path": ["label"], "type": "string"}
     ]},
    {"name": "readings", "parent": "visits", "path": ["readings"],
     "columns": [
       {"name": "code", "path": ["code"], "type": "string"},
       {"name": "value", "path": ["value"], "type": "integer"}
     ]},
    {"name": "consents", "parent": "participants", "path": ["consents"],
     "columns": [
       {"name": "kind", "path": ["kind"], "type": "string"}
     ]}
  ]
}
```

语义与硬性规则：

- 一张根表、任意张子表。每张子表声明 `parent`（根表名或另一张子表名）
  与 `path`：从**父表对象**出发到**目标数组**的相对路径。每次跨越重复数组
  都必须另建子表；普通列路径可以穿过对象，**不能穿过数组**（把数组写进
  列路径会被拒绝，而不是偷偷展开）。
- 路径是**字符串段数组**。键名里的点、空格、斜杠（`"odd.key/name"`、
  `["meta","tags"]` 等）都是普通字符。不拼接字符串、不猜层次；空段被拒绝。
- 每列声明从**当前对象**（根对象或子表数组的一项）出发到标量的相对路径
  与 `type`：`string` / `integer` / `decimal` / `boolean`。`integer` 与
  `boolean` 分开；`integer`/`decimal` 以**精确十进制文本**落库，不经过二进制
  浮点，大整数与小数位数不丢失。
- 表名、列名在方案内唯一；表名不得以 `ns_` 开头；列名不得等于内部身份列
  （`row_id` / `parent_row_id` / `root_row_id` / `source_digest` /
  `root_index` / `index_path` / `array_path` / `ordinal`），也不得以
  `__present` 结尾（存在标记列保留）。同一表内两列不得投影同一路径。
- 同一键位置只能是一种角色：标量列、中间对象、子数组，三者互斥；同一父表
  的两张子表不得指向同一数组。**冲突定义**整份方案拒绝。
- 编译先于读取：未知父表、父子成环、重复表/列、冲突定义、非法类型或路径
  都在**读任何来件字节之前**报错（退出码 2），不会建库、不会读入件。

### 13.1.1 题目例子的实际方案

对输入

```json
{"person":"P1","study":"S1",
 "visits":[{"label":"V1","readings":[{"code":"A","value":0},{"code":"A","value":2}]},
           {"label":"V2","readings":[]}],
 "consents":[{"kind":"lab"},{"kind":"contact"},{"kind":"reuse"}]}
```

（顶层省略为单元素数组）完整的方案为：

```json
{
  "version": 1,
  "root": {
    "name": "participants",
    "columns": [
      {"name": "person", "path": ["person"], "type": "string"},
      {"name": "study", "path": ["study"], "type": "string"}
    ]
  },
  "tables": [
    {"name": "visits", "parent": "participants", "path": ["visits"],
     "columns": [
       {"name": "visit_label", "path": ["label"], "type": "string"}
     ]},
    {"name": "readings", "parent": "visits", "path": ["readings"],
     "columns": [
       {"name": "code", "path": ["code"], "type": "string"},
       {"name": "value", "path": ["value"], "type": "integer"}
     ]},
    {"name": "consents", "parent": "participants", "path": ["consents"],
     "columns": [
       {"name": "kind", "path": ["kind"], "type": "string"}
     ]}
  ]
}
```

投影结果：`participants` 1 行（P1/S1）、`visits` 2 行（V1、V2；V2 的空
readings 不生成读数行但保留访视行）、`readings` 2 行（都属于 V1，code 相同
也不去重）、`consents` 3 行。读数列与授权列是两个分支，不会相乘。
`demo/expected_nested.py` 里这份期望（行身份、父行、坐标、值与存在标记）
全部手写，并由 `demo/verify_nested.py` 用自己的 SQL 逐项核对。

### 13.2 输入与投影语义

- 顶层是一个 JSON 数组，每项是一个根对象；子数组的每项**必须是对象**。
  `[1, 2]`、`"x"`、`{...}` 这类顶层或项形状错误按源级错误/根级拒绝处理。
- **数组缺失或为空**：不生成子行（但父行照常保留）。**显式 `null` 或其它
  类型放在数组位置**：根记录被拒（`array_is_null` / `array_expected`）。
  **列路径缺失**与**显式 `null`** 都合法，但落库可区分：每列有
  `col__present`，`0`=缺失、`1`=有值（含 `0`/`false`/`""`）、`2`=显式 null。
- 对象里**重复同一个 JSON 键**：整条根记录被拒（`duplicate_key`），绝不靠
  “最后一个值覆盖前一个值”（这是一个独立的键集合检查，不依赖解析器行为）。
- 未知字段不参与投影（但仍参与重复键检查与结构合法性）。**类型不符**拒绝
  根记录，不做隐式文本/数值转换。
- 读数列与授权列是两条**独立分支**，各自按自己的数组下标生成行——不会相乘
  成笛卡尔积，也不会因为两行的业务值相同而去重：内容相同的两项仍是两行；
  不同文件里相同的 `person` 也不会自动合并。

### 13.3 行身份、来源与顺序

- 每行身份 `row_id = sha256(json([source_digest, root_index, array_path]))`，
  与业务值无关、可重复计算。`source_digest` 是**原始文件字节**的 sha256；
  `array_path` 是从根到该行的**完整数组下标路径**（每一层写成
  `[子表名, 下标]`，根行为 `[]`），因此不同子表在同一数字下标下也不会撞车。
- 每行另有 `parent_row_id`（直接父行身份，根行为空）、`root_row_id`、`source_digest`、
  `root_index`、`index_path`（下标数组）与 `ordinal`（本表本根内创建顺序）。
  来源位置能把一行查回**同一个根**和**同一个数组项**。
- `read` 按 `(root_index, ordinal, row_id)` 逐行输出，可复现输入顺序；
  `children --parent ROW_ID` 按同样顺序给出直接子行。每行都带
  `row_id / parent_row_id / root_row_id / source{...} / values{col:{value,present}}`。
  读取逐游标流式进行，不把整棵树重新拼成内存对象。

### 13.4 落库边界、错误与续办

- **一条根记录及其所有后代是一个事务边界**。任一错误撤销本条根已生成的
  全部父子行，记录 `root_index`、路径与错误类别（`duplicate_key` /
  `type_mismatch` / `array_is_null` / `array_expected` / `item_not_object` /
  `root_not_object` / `structure_mismatch` / `number_out_of_range` /
  `nesting_too_deep` / `object_keys_exceeded`），随后继续处理下一条结构完整
  根。诊断只含序号、标识路径与类别——**不含**姓名、读数、备注等载荷。
- **JSON 语法本身坏**（截断、非法 token、尾随数据、非 UTF-8）：当前根回滚、
  停止并明确报告无法继续的**字节位置**（略有：`exact_offset` / `end_of_file` /
  `upper_bound`）。以前已提交的根保留，当前根不留半棵树；不猜下一条记录
  的开头。
- 方案绑定到**产物库**（`meta.plan_digest`）：换方案会 `plan_mismatch`，不会
  沿用同一进度。以**源文件摘要**登记进度；同一文件重送或中断后重开时，已
  提交的根按 `root_index` 跳过、不再增加行；未提交根从头重处理。允许整文件
  从头流式扫描并跳过已确认根，不要求随机跳字节。
- 摘要必须在决定“跳过哪些根”之前确定，所以 `prepare` 先把源文件**流式
  spool 到 store 的 `tmp/` 并同时算 sha256**（内存仍是一个读块），再从头解析
  spool 文件；结束即删。意外退出留下的 `nested.<pid>.*.input` 由 `reconcile`
  按属主 pid 回收（临时文件永不作为已接收凭据）。产物库里的 `ns_runs` /
  `ns_roots` 只存摘要、序号、状态与错误类别。
- 每一行写入与“该根已提交”的持久记录在**同一事务**提交，故“后代已写入、
  根未提交”处强退时该根整棵树随事务消失（确定性强退）；恢复后行身份与父子
  归属不变。相同源换协议或换方案、或源字节改变（摘要不同）都不会误接原进度。
  源文件大小与登记不符时报 `source_changed`（退出码 4）。

### 13.5 解析栈与内存边界

按**解析事件**处理，不用 `ijson.items` 组装整对象，也不整文件读入：

| 状态 | 内存边界 |
|---|---|
| 解析器自身 | 一个读块（`--chunk-size`，默认 64 KiB） |
| 结构栈（对象/数组帧） | 每个**当前打开**的容器一帧；嵌套深度硬上限 `MAX_NESTING`（+方案深度），超限拒绝该根 |
| 未决父字段 | 每个当前路径层级最多一个行帧，各含**该表方案列数**个槽位；父对象的标量无论写在子数组**之前还是之后**，代价都是 O(方案宽度)，与子数组长度无关（写入子行前已算好父行身份，父行在对象结束时插入） |
| 重复键检查 | 每个**打开的**对象一个键字符串集合；单对象键数超 `MAX_KEYS_PER_OBJECT` 拒绝该根（随单个对象增长，不随文件增长） |
| 诊断 | 每根仅首错：类别 + 标识路径 |
| 事务暂存 | 一根的所有行在一个 SQLite 事务内；页缓存受 `PRAGMA cache_size` 约束（8 MiB），回滚日志在磁盘——内存有界，仅磁盘随根大小增长 |

即：不是只把读取改小块就称有界；上述每一处状态都有明确界，且整棵内存对象树
从不组装。演练实测 3.8 MB / 12 万读数单根文件在 200 MB 地址空间上限下峰值
RSS ≈ 30 MB。

### 13.6 演练与独立校验

```
python3 demo/run_nested_demo.py     # 全部工件在临时目录，启动时打印路径
```

`demo/expected_nested.py` 是**手写**期望（行身份按身份规则在期望文件里独立
重算，来源摘要由字面量算；投影值、存在标记、父行与坐标全部是字面量）；
`demo/verify_nested.py` 用**自己的 SQL** 直接读回产物库逐字段比对，不调用正式
整备器计算答案。阶段：

| 阶段 | 内容 |
|---|---|
| 1 | 方案编译与全部拒绝类别（未知父表/成环/重复表/重复列/冲突定义/重复目标/保留列名/非法类型/非法路径）；坏方案在**动输入前**被拒且不建库 |
| 2 | 题目例子：1 参加者 / 2 访视 / 2 读数 / 3 授权，逐项父子键与坐标一致；`read`/`children`/`roots`/`status` 接口 |
| 3 | 父字段（person/study）写在子数组**之后**：投影值与早写版逐列相同 |
| 4 | 空数组、缺失、null、false、0、空串、内容重复的授权/读数、带 `/`、`.`、空格的键名 |
| 5 | 中间坏根（重复键 / 类型错）：坏根整树消失，前后好根仍可读，诊断不含载荷 |
| 6 | 末尾语法截断：无半棵树，已提交根保留，报告中止位置；续办不翻倍 |
| 7 | 在“后代行已写入、根未提交”处 SIGKILL 后 `resume`：不翻倍、身份与父归属不变；重送同字节全跳过；换方案 / 换源字节不误接 |
| 8 | 产物库换方案被拒（`plan_mismatch`），原库未被改动 |
| 9 | 少量根、单根 12 万后代：在 200 MB 地址空间上限下实跑，核对各表总行数、首尾子项与实测峰值 RSS |
| 10 | 旧命令（import/query/mail status）照旧；嵌套产物从不写入 `vault.sqlite3` 业务表 |

退出码沿用 §9：成功 0；方案/结构/换方案或换源等拒绝 2；`resume` 未知源
或数据库不可读 4。逐阶段说明与手算期望见 `demo/NESTED_DEMO.md`。

## 14. 字节对词表学习与编码（bpe）

研究备注进入后续文本处理前，需要把原文变成统一词表下的整数序列。本子系统是
**独立模块**：用 Python 与 SQLite 把训练状态落在自己的数据库文件里（从不写入
`vault.sqlite3`、不发变更通知、不动观测/撤回协议）。它只学习**文本预处理词表**：
不训练语言模型、不计算研究结论；只处理调用方显式给出的语料文件，**从不**从历史
观测或已撤回资料提取正文。除标准库外无第三方依赖。

```
rvault bpe train  --database DB --input FILE --max-merges N --min-count N [--lock F]
rvault bpe resume --database DB [--input FILE] [--max-merges N] [--min-count N] [--lock F]
rvault bpe status --database DB [--lock F]
rvault bpe rounds --database DB [--lock F]
rvault bpe vocab  --database DB [--output FILE | -] [--lock F]
rvault bpe encode --vocab FILE --input FILE --output FILE
rvault bpe decode --vocab FILE --input FILE --output FILE
```

`--database` 是自建训练库；`--lock` 默认 `<database>.lock`（flock 文件，进程死亡
由 OS 释放，无需手删）。`bpe` 不要求 `--store`，其余命令行为不变。

### 14.1 训练文件与确定性规则

逐行 JSONL，每行字段**恰好**为 `id / weight / text`：`id` 是非空字符串且文件内
唯一；`weight` 是正整数且**不能是 bool**（bool 是 int 子类，显式拒绝）；`text` 是
字符串。`text` 按原值转 UTF-8 字节——**不做**大小写折叠、Unicode 规范化或空白
清洗。坏 JSON、缺字段、多字段、类型不符、重复 id、空文件均带**行号**拒绝，且整份
语料在写入前完成校验，被拒时不创建数据库、不留半份语料。

固定算法（不同机器重做得到完全相同的词表）：

1. 初始 token 0..255 各代表一个字节。
2. 每轮挑当前语料中**加权相邻次数**最大的 token 对，次数相同按
   `(left_id, right_id)` 从小到大决胜。
3. 达到 `max_merges`，或最高次数低于正整数 `min_count`，即停止。
4. 新 token 依次编号 256、257……，词表保存它由哪两个旧 token 组成及合并轮次。

相邻次数**包括重叠位置**；执行替换时从每条文本左侧开始替换**不相交**位置，绝不
跨越两条文本的边界。只有 `id=x, weight=2, text="aaaa"` 时：

| 轮次 | 选中对 | 加权次数（含重叠） | 替换位置数 | 替换后序列 |
|---|---|---|---|---|
| 1 | (97,97)→256 | 3 个位置 × 权重 2 = **6** | 2 | [256,256] |
| 2 | (256,256)→257 | 1 × 2 = **2** | 1 | [257] |

两轮的次数 6 和 2 不能混算。两条内容一样但 id 不同的语料分别按各自权重贡献；
空文本合法但没有相邻对。所有次数/权重运算是**精确整数**：支持范围（单权重、
权重总和、加权相邻位置总数、轮数）在**读入整个文件时**检查，超界即拒绝，后续
合并不会溢出也不使用浮点近似。

### 14.2 磁盘状态与局部更新（重点）

训练状态全部在 SQLite（表前缀 `bp_`）：

| 表 | 内容 |
|---|---|
| `bp_meta` | 输入字节摘要（sha256）、`min_count`、轮数上限、next token、轮次计数、状态机 |
| `bp_docs` | 每条语料：id、行号、weight、原始 UTF-8 字节、**当前** token 序列（uint32 打包 BLOB）、长度 |
| `bp_pairs` | 当前每个相邻对的精确加权次数 |
| `bp_pair_hits` | 哪个对命中哪些文本（命中关系） |
| `bp_vocab` | 256 起每个新 token 的左右来源与合并轮次 |
| `bp_rounds` | 每轮：选中对、加权次数、访问/改写文本数、替换位置数（可查询统计） |
| `bp_round_*` | 未确认轮次的暂存区（计划、逐文本新旧序列、增量、新命中） |

**不是**每轮重扫全部文本。完成初始计数后，每轮：

1. 用 `INSERT ... SELECT` 直接从命中关系物化本轮计划（不把候选 id 装进 Python
   列表）；
2. 用游标**只重读包含选中对的文本**，一条文本及其序列驻内存，撤回它此前的相邻
   次数贡献、做左到右不相交替换、暂存新贡献；一条文本里同一对出现多次也只在本轮
   重写一次；
3. 一个确认事务里同时生效：次数增量、命中关系替换、当前序列更新、词表追加、
   `bp_rounds` 统计与 next token 推进。

### 14.3 崩溃停靠点、续办与并发

- 提供确定性停靠点（同步点 `bpe.round.after_doc`、`bpe.round.before_confirm`，
  机制同 §8）：可以在部分命中文本已改写、该轮**尚未确认**时终止进程。
- 重开 `resume` 时丢弃暂存区、把该轮**完整重做**：主表（次数/命中/序列/词表）在
  确认前从未被修改，所以不会漏文本、不会重复扣次数、也不会多占 token 编号。
- 训练库记住输入字节摘要与影响结果的参数：不同语料（`corpus_mismatch`）或不同
  `min_count`（`parameter_mismatch`）不能接在旧状态上；不带 `--input` 也能凭库内
  身份续办。
- 同一训练库的推进由 flock 串行：两个推进进程相遇时后到者阻塞等待，不会推进出
  不同的合并轮次；占用方退出（含 SIGKILL）后锁自动释放，可继续，不靠手删锁文件。
- 合并轮次的改写、统计与词表追加在同一事务提交，要么整轮保留要么整轮重做。

### 14.4 词表格式与编码/解码（只保证可逆）

`bpe vocab` 导出自描述 JSON：

```json
{"format": "rvault-bpe-vocab", "version": 1, "base_token_count": 256,
 "corpus_digest": "...", "min_count": 1,
 "merges": [{"token": 256, "left": 97, "right": 97, "round": 1}, ...],
 "vocab_digest": "<对其余字段规范化后的 sha256>"}
```

编码文件首行是绑定头 `{"format":"rvault-bpe-encoded","version":1,
"vocab_digest":...}`，随后每行 `{"id","tokens"}`。编码的 `text` 不必出现在训练
语料里：按已学合并的先后顺序应用同样的非重叠替换，逐条**流式**处理。基础字节
覆盖所有有效 UTF-8 文本，**不引入未知词替代**。

- 解码按 `vocab_digest` 绑定：选错词表不能静默解出另一段文字（`vocab_mismatch`）；
  词表文件被改动则自身摘要不符（`vocab_tampered`）；未知 token（`unknown_token`）、
  编码文件格式错误、或展开后的字节不是合法 UTF-8（`invalid_utf8`）一律明确拒绝。
- 用词表解码必须**逐字符还原**原 `text`，包括组合字符、表情、空格、换行与空
  字符串。
- **只保证可逆，不保证压缩率**：若语料缺乏可合并模式，token 数可能不少于字节数。

### 14.5 演练与独立校验

```
python3 demo/run_bpe_demo.py     # 10 个阶段，全部工件在临时目录
```

`demo/expected_bpe.py` 是手写期望（aaaa 两轮与丰富语料逐轮的选中对/次数）；
`demo/bpe_slow_ref.py` 是**另外编写的慢速参考算法**，每轮全量重新计数、全量重扫
替换，**不复用**正式计数与替换函数；`demo/verify_bpe.py` 用自己的 SQL 直接读回
训练库，从原始字节重放全部合并并核对序列、次数、命中关系、词表与元数据。阶段：

| 阶段 | 内容 |
|---|---|
| 1-2 | aaaa 手算两轮；同分决胜、相同正文不同 id、跨文本假相邻对、空文本、非 ASCII，逐轮读回 |
| 3 | 慢速参考 vs. 磁盘增量引擎：打乱行序 × 含重叠重复对的多组材料，整个合并序列与最终 token 序列逐项相同；参考编码器在未见文本上一致 |
| 4 | 编码/解码字节级还原（组合字符、ZWJ 表情、空格、换行、空串） |
| 5 | 轮中 SIGKILL 后接续：无漏文本、无重复扣次数、无多占 token；含双重崩溃 |
| 6 | 两个推进进程在 flock 相遇：一个等待，库内只有一条一致合并序列、token 编号连续 |
| 7 | 坏行/重复 id/bool 与浮点 weight/非法类型带行号拒绝且不留库；改语料或 min_count 被拒 |
| 8 | 错误词表解码、词表篡改、未知 token、非 UTF-8 展开、坏文件头全部明确拒绝 |
| 9 | 30020 条稀疏语料（20 热 + 30000 冷）：每轮实际只读写 20 条热文本，冷文本零改写；记录每轮读写数与峰值 RSS |
| 10 | 既有 init/import/query/withdraw/nested 命令路径不受影响 |

退出码沿用 §9：成功 0；输入/参数/身份冲突拒绝 2；未知数据库、输入或词表 4；
词表或编码文件完整性失败 5。

