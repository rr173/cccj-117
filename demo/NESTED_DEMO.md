# 嵌套研究表整备 —— 演练复跑文档

本文件说明如何直接复跑嵌套整备的完整演练，以及每一步应看到的结果。所有
期望值都是**手写**的（`demo/expected_nested.py`），行身份在期望文件里按
身份规则独立重算，来源摘要由字面量字节算出，不经过被测实现。

## 1. 一键复跑

```sh
cd <仓库根目录>
python3 demo/run_nested_demo.py
```

- 演示（-demo）需要 `ijson`：`pip install ijson`。旧命令不依赖它。
- 所有工件（方案文件、输入 JSON、store、产物库、日志、SIGKILL 同步点标记）
  都写在 `tempfile.mkdtemp(prefix="rvault-nested-")` 创建的临时目录里，启动时
  打印；不会写进仓库。
- 用 `RVAULT_NESTED_WORK=/path/to/dir` 可指定目录（目录须已存在或可创建）。
- 成功时最后一行是 `NESTED DEMO COMPLETE`，退出码 0。

逐命令日志在 `<work>/logs/NNN_*.out|err`，校验器日志在
`<work>/logs/verify_*.log`，大文件探针结果在 `<work>/logs/big_probe.json`。

## 2. 独立校验器

演练在阶段边界调用：

```sh
python3 demo/verify_nested.py --database <work>/example.sqlite3  --checkpoint example
python3 demo/verify_nested.py --database <work>/late.sqlite3     --checkpoint late
python3 demo/verify_nested.py --database <work>/variants.sqlite3 --checkpoint variants
python3 demo/verify_nested.py --database <work>/bad_middle.sqlite3 --checkpoint bad_middle
python3 demo/verify_nested.py --database <work>/bad_type.sqlite3 --checkpoint bad_type
python3 demo/verify_nested.py --database <work>/truncated.sqlite3 --checkpoint truncated
python3 demo/verify_nested.py --database <work>/resume.sqlite3   --checkpoint resume
python3 demo/verify_nested.py --database <work>/resume.sqlite3   --checkpoint plan_mismatch
python3 demo/verify_nested.py --database <work>/big.sqlite3      --checkpoint big
```

校验器用**自己的 SQL** 直接读回产物库，逐行比对 `row_id / parent_row_id /
root_row_id / source{root_index,index_path,array_path,ordinal} / 每列
{value, presence}`，任一不符即非零退出。它不导入 `rvault` 任何模块。

## 3. 期望清单（手算）

### 3.1 题目例子

输入（`F_EXAMPLE`，见期望文件的字面量）根记录：
`P1 / S1 / visits=[V1(readings=[A/0, A/2]), V2(readings=[])] /
consents=[lab, contact, reuse]`。

| 表 | 行数 | 行身份（array_path） | 父行 |
|---|---|---|---|
| participants | 1 | `[]` | – |
| visits | 2 | `[["visits",0]]`、`[["visits",1]]` | 根行 |
| readings | 2 | `[["visits",0],["readings",0/1]]` | V1 |
| consents | 3 | `[["consents",0/1/2]]` | 根行 |

值：`person=P1`、`study=S1`；visit_label 依次 V1、V2；reading 依次
`(A,"0")`、`(A,"2")`（相同 code 不去重）；kind 依次 lab、contact、reuse。
`V2` 无读数不生成读数行，但访视行保留。读数列与授权列是独立分支，不会相乘。

### 3.2 父字段后置

`F_LATE` 把 `person/study` 放到 `visits`、`consents` 之后。投影值与坐标与
§3.1 完全相同，仅 `source_digest`（因此行身份）不同。

### 3.3 存在状态与重复值（`F_VARIANTS`）

| 位置 | 输入 | presence |
|---|---|---|
| root0.ratio | `0` | 1（值 `"0"`） |
| root0.memo | `""` | 1（值 `""`） |
| root0.visits_after | `0` | 1 |
| root1.ratio | `null` | 2（显式 null） |
| root1.memo | `null` | 2 |
| root1.study | 缺失 | 0 |
| root1.display.label | 缺失 | 0 |
| root0.consents[0..1].granted | `false` | 1（值 `0`） |

两个相同授权仍两行；两个相同读数（同 code/同 value）仍两行；`meta.tags`
两项的键含 `/`、`.` 与空格，各自按字面段投影，`array_path` 仍是
`[["tags",0]]`、`[["tags",1]]`。

### 3.4 中间坏根（`F_BAD_MIDDLE` / `F_BAD_TYPE`）

- `F_BAD_MIDDLE`：根 1 重复 `person` 键 → `duplicate_key`；根 0、2 提交。
  存活 person = `R0`、`R2`；根 1 在任何表里 0 行；诊断 `path=/person`，
  不回显 `R1`/`R1b`。
- `F_BAD_TYPE`：根 1 的 `person` 是数字 7 → `type_mismatch`；根 0、2 提交。
  存活 person = `R0`、`R2`；诊断不含 `7`。

### 3.5 末尾截断（`F_TRUNCATED`）

根 0 已提交（1 行 participant，`T0`）；根 1 从未提交（不存在任何行或记录）。
运行状态 `aborted_syntax`，类别 `json_syntax`，位置=文件长度，
`position_basis=end_of_file`，`root_index=1`。续办不翻倍。

### 3.6 强退续办（`F_RESUME`，3 根）

在**根 1** 的 `nested.root_before_commit.1` 同步点 SIGKILL（根 0 已提交，
根 1 的子行已写入但事务未提交）：

- 库内只剩根 0 的提交记录与 1 行 participant；根 1 及其后代行全部消失。
- `resume` 跳过根 0（1 跳过），处理根 1、2（2 提交），最终 3 根各一次，
  各表 3 行，行身份与父行与直接跑完一致。
- 再重送同字节：3 根全部跳过、0 新增。
- 换方案：`plan_mismatch`（退出码 2）。
- 换源字节（同长度、改一个 person）：新摘要，不复用旧进度。

### 3.7 大文件（`big.json`）

3 根；根 1 有 2 次访视、每次 60000 读数（共 120000），另有根 0、2 各 1
读数，故 `readings` 总计 120002，`visits` 4，`consents` 3，`participants` 3。
第一条读数是 `C00000/0`；全局最后一条是 `C%05d`（按 100000 取模）/
value=119999。演练在 **200 MB 地址空间上限**下实跑并报告实测峰值 RSS
（通常 ≈ 30 MB）。

## 4. 方案拒绝清单（编译阶段）

`PLAN_REJECTIONS`（每个都应在读任何输入前以退出码 2 拒绝，且不建库）：

`unknown_parent` / `parent_cycle` / `duplicate_table` / `duplicate_column` /
`conflicting_definition`（标量↔中间对象、标量↔子数组、中间对象↔子数组）/
`duplicate_table_target`（同一父表两张子表指向同一数组）/ `reserved_table_name`
（`ns_*`、`sqlite_*`、`meta`）/ `invalid_column_type` / `invalid_path`。
