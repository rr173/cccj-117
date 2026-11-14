# 版本化输入契约 —— 演练复跑文档

本文件说明如何**直接复跑**版本化输入契约的完整演练，以及每一步应看到的
结果。期望值全部是手算字面量（`expected_contracts.py`），不经过被测实现。

## 1. 一键复跑

```sh
cd <仓库根目录>
python3 demo/run_contract_demo.py
```

- 所有工件（store、临时生成的契约文件、输入 JSONL、日志、同步点标记）
  都写在 `tempfile.mkdtemp(prefix="rvault-contract-")` 创建的临时目录里，
  启动时打印；不会写进仓库。
- 用 `RVAULT_CONTRACT_WORK=/path/to/dir` 可指定目录（目录须已存在或可创建）。
- 成功时最后一行是 `all contract checks passed`，退出码 0。

逐阶段日志在 `<work>/logs/NNN_*.out|err`，校验器日志在
`<work>/logs/verify_*.log`。

## 2. 独立校验器

演练在阶段边界调用：

```sh
python3 demo/verify_contracts.py --store <work>/store --checkpoint cp_initial
python3 demo/verify_contracts.py --store <work>/store --checkpoint cp_register
python3 demo/verify_contracts.py --store <work>/store --checkpoint cp_import
python3 demo/verify_contracts.py --store <work>/store --checkpoint cp_rejected
python3 demo/verify_contracts.py --store <work>/store --checkpoint cp_legacy
python3 demo/verify_contracts.py --store <work>/store --checkpoint cp_withdrawn
```

校验器用自己的 SQL 直接读 `ct_contracts` / `ct_fields` / `editions` /
`ct_batches` / `cf_events` / 回执文件，逐个元组比对 `expected_contracts.py`
中的手算值；任一不符即非零退出。

## 3. 期望清单（手算）

### 3.1 契约版本塔

| 提供方 | 资料类型 | 版本 | 前版 | 兼容声明 | 变更清单（相对前版） |
|---|---|---|---|---|---|
| labA | obs | 1 | – | initial | 7 个 `field_defined` |
| labA | obs | 2 | 1 | compatible | `note nullable_relaxed`、`site field_added`、`value type_widened`（全部 compatible） |
| labA | obs | 3 | 2 | branch | `flag field_added`(false)、`note field_removed`(false)，`classification=branch_required` |
| labB | measurement | 1 | – | initial | 9 个 `field_defined`（含 `verified:boolean` 与可选 `batch:integer`，字段名 `subject/sample/attempt/title/reading/remark`） |
| labB | measurement | 2 | 1 | compatible | `batch type_widened` |

（另有私有 `labRej` 克隆 v1+v2，仅用于拒绝清单，不计入版本塔。）

### 3.2 导入结果（规范化存储元组）

`editions` 行 = `(study, observation, edition, person, display_name, measure, memo, batch_tag)`

```
(laba, L1, 1, pa-1, Alpha One,       10,   "",       a-good)
(laba, L1, 2, pa-1, Alpha One again, 11,   "second", a-good)
(laba, L2, 1, pa-2, Alpha Two,       20,   "",       a-good)
(laba, L3, 1, pa-3, Alpha Three,     10.5, "north",  a-good)
(labb, M1, 1, pb-1, Beta One,        7,    "",       b-good)
(labb, M2, 1, pb-2, Beta Two,        8,    "see note", b-good)
(laba, B1, 1, pa-9, Branch,          3,    "checked", v3-explicit)
(legacy, G1, 1, pl-1, Legacy One,    100,  "m1",     legacy-good)
```

- `L1 e1` 的 `memo=""`：来自契约默认值（`note` 省略）。
- `L2 e1` 的 `memo=""`：`note` 显式 `null`，`nullable` 合法，落默认。
- `L3 e1` 的 `measure=10.5`：v2 把 `value` 拓宽为 number，值原样存储。
- `B1` 只由显式 `--contract-version 3` 接收。

### 3.3 整批拒绝（阶段 7）

一批 4 行，前 3 行合法、末行含未知字段 `secret` 且 `value` 类型错误：

- 结果：`status="rejected"`，退出码 2；
- 诊断恰为 `(line=4, field=secret, reason=unknown_field)` 与
  `(line=4, field=value, reason=type_mismatch)`；
- `editions` 中没有 K1/K2/K3；`batches` 没有 `mixed-bad`；`receipts/` 无
  该批次回执；`cf_events` 没有新增；`tmp/` 为空；
- 诊断 JSON 中不出现 `LEAK-NAME` / `LEAK-FIELD` / `LEAK-MEMO`。

### 3.4 无契约导入（阶段 9）

不带选择器的 `import` 回执**没有** `protocol` / `contract` 字段，
`measure` 仍必须是整数，仍产生 `refresh` 通知，`ct_batches` 记
`protocol="legacy"`。

### 3.5 竞争同一后继版本（阶段 8）

两个进程同时发布 labB/measurement v3（前版 2）：

- 先拿到写锁者 `status="recorded"`；
- 后者阻塞（休眠 1 秒后仍存活），前者提交后它以 `content_conflict` 退出 2；
- 重开数据库后 `ct_contracts` 只多一行；`ct_fields` 中 v3 的字段集完全等于
  胜者文档（不是两份文档的合并）。

## 4. 手工复跑（同一组命令逐条执行）

完整命令与手算答案见 `README.md` §11.5；把其中的 `/tmp/ct` 换成任意空
目录即可。关键检查点：

```sh
# 比较：应输出 classification=compatible 与三项 compatible 变更
contract compare --provider labA --material-type obs --against 1 --fields v2.json
# 以 compatible 提交不兼容文档：退出码 2，code=incompatible_change
contract register ... --version 3 --predecessor 2 --compatibility compatible
# 以 branch 提交：退出码 0，compatibility=branch
contract register ... --version 3 --predecessor 2 --compatibility branch
# 查看已发布版本（字段定义 + 已登记的变更清单）
contract show --provider labA --material-type obs --version 3
# 查看整条契约线
contract list --provider labA --material-type obs
# 导入并按该版本检查
import --batch-tag b1 --provider labA --material-type obs --contract-version 2 ...
```

## 5. 拒绝类别速查

| 触发 | 错误码（退出码 2） |
|---|---|
| 同版本不同内容重送 | `content_conflict` |
| 跳号 / 复用编号 / 首版非 1 | `version_gap` |
| 后继版本未给出前版 | `predecessor_required` |
| 前版不存在（含首版带前版） | `unknown_predecessor` |
| 自指（版本把自己指为前版） | `cycle_detected` |
| 前版指针图成环（防御性链走；正常 CLI 无法构造，演练用损坏的私有库复现） | `cycle_detected` |
| 兼容升级含不兼容变更 | `incompatible_change` |
| 字段定义非法（空字段名、空默认值、类型/目标不合法等） | `invalid_contract` |
| 选择器非法（空提供方/资料类型、非正整数版本） | `invalid_contract_selection` |

导入时未知契约版本 → `unknown_contract_version`，退出码 **4**；
其余逐行契约违规 → 整批拒绝，退出码 2，诊断含 `line/field/reason`。
