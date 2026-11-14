# 离线邮件来件通道 —— 演练复跑文档

本文件说明如何**逐条复跑**邮件通道演练中的每个操作，以及每一步应看到的
结果。期望值全部是手算字面量（`demo/expected_mail.py`），不经过被测实现；
校验器 `demo/verify_mail.py` 用自己的 SQL 直接读库比对。

## 1. 一键复跑

```sh
cd <仓库根目录>
python3 demo/run_mail_demo.py
```

- 所有工件（store、生成的 .eml 样本、日志、同步点标记）都写在
  `tempfile.mkdtemp(prefix="rvault-mail-")` 创建的临时目录里，启动时打印；
  不会写进仓库。用 `RVAULT_MAIL_WORK=/path/to/dir` 可指定目录。
- 成功时最后一行是
  `MAIL DEMO COMPLETE  (big-message receive peak RSS: NNNN kB)`，退出码 0。
- 逐阶段日志在 `<work>/logs/NNN_*.out|err`，校验器日志在
  `<work>/logs/verify_*.log`；样本邮件在 `<work>/samples/*.eml`。

## 2. 样本邮件（演练生成的真实 MIME 字节）

| 文件 | 内容 |
|---|---|
| `mail1.eml` | multipart/mixed：说明文字（含 JSON 示例与边界相似行）→ multipart/related（html + base64 PNG）→ base64 数据附件（CRLF 行尾、非 ASCII、末尾无换行）→ quoted-printable 数据附件（软换行、LF 行尾、末尾有换行）→ 清单（8bit） |
| `e1_*.eml` … `e11_*.eml` | 十种预检错误样本（见 §4 表） |
| `partial.eml` | 三份数据附件 + 清单：pb1 合法、pb2 第 2 行 `edition: 0` 非法、pb3 合法 |
| `crash.eml` | 三份数据附件（binary / base64 / QP）+ 清单，用于强退续办 |
| `safe.eml` | 数据附件文件名 `../../data.jsonl`、清单文件名 `../../manifest.json` |
| `withdraw.eml` | 一份 base64 附件（屏障内 p-dan + 新参与者 p-new 两行）+ 清单 |
| `big.eml` | 一份 base64 大附件（120 000 行）+ 清单；约 19.7 MB |
| `unsent.eml` | 一封从未被接收的邮件，用于 `resume` 未知邮件用例 |

## 3. 逐条复跑与期望结果

以下命令假设 `W=<work 目录>`（演练启动时打印）。每条都可单独重跑，
结果确定。

### 3.1 主邮件接收（对应阶段 1）

```sh
PYTHONPATH=. python3 -m rvault --store $W/m1 mail receive --input $W/samples/mail1.eml
```

期望（退出码 0）：

- `status: "received"`，`manifest_part: "5"`，`part_count: 7`；
- `batches` 两条，均 `status: "accepted"`：`mail-b1`（part `3`，
  accepted 3）、`mail-b2`（part `4`，accepted 2）；
- 两条的 `sha256` 分别等于 `expected_mail.py` 的 `B1_SHA` / `B2_SHA`
  （解码字节摘要与手算载荷摘要一致）。

库内核对（`verify_mail.py --checkpoint main` 做的就是这些 SQL 比对）：

- `editions` 恰 5 行：site-alpha e1/e2（p-ann，10/11）、site-beta e1
  （p-bob，20）、site-delta e1（p-dan，40）、site-gamma e1（p-cid，30），
  display_name/memo 含 `Ann Müller`、`first café`、`Bob 林`、`Cid 王` 等
  非 ASCII 原文；
- `cf_events` 恰 4 条 refresh：帧 1 = {site-alpha, site-beta}，
  帧 2 = {site-delta, site-gamma}；
- `ml_batches`：(1, mail-b1, part 3, accepted)、(2, mail-b2, part 4,
  accepted)；`ml_messages.state = "done"`；
- `$W/m1/tmp/` 为空。

### 3.2 小读块对照（阶段 2）

```sh
PYTHONPATH=. python3 -m rvault --store $W/m1s mail receive \
    --input $W/samples/mail1.eml --chunk-size 7
```

期望：与 3.1 完全相同的结果（退出码 0）；校验器对 `m1s` 重放
`main` 检查点，且演练直接比较两个 store 的批次摘要 / editions /
通知 / 处理记录——全部逐项相同。

### 3.3 预检错误（阶段 3）

```sh
PYTHONPATH=. python3 -m rvault --store $W/errstore mail receive \
    --input $W/samples/<case>.eml        # 每条退出码都是 2
```

| 样本 | 期望 `errors[].category` | 首错 `part` |
|---|---|---|
| `e1_digest_mismatch.eml` | `digest_mismatch` | `1` |
| `e2_duplicate_cid.eml` | `content_id_duplicate`, `reference_ambiguous` | – |
| `e3_unterminated.eml` | `unterminated_multipart` | `0` |
| `e4_unsupported_cte.eml` | `unsupported_transfer_encoding` | `1` |
| `e5_invalid_encoding.eml` | `invalid_transfer_encoding` × 2 | `1` |
| `e7_rfc822.eml` | `manifest_missing`（不递归内嵌邮件） | – |
| `e8_manifest_duplicate.eml` | `manifest_duplicate` | – |
| `e9_manifest_invalid.eml` | `manifest_invalid` | – |
| `e10_batch_tag_duplicate.eml` | `batch_tag_duplicate` | – |
| `e11_reference_unresolved.eml` | `reference_unresolved` | – |

共同期望：`status: "rejected"`；输出里**不出现**载荷值
（样本载荷的 display_name 统一含 `DO-NOT-LEAK`，演练逐条断言它不出现在
任何诊断里）；`errstore` 的 `editions`/`batches`/`cf_events` 全部为空；
`tmp/` 无残留。重收同一被拒邮件结果相同（幂等）。

### 3.4 越界文件名（阶段 4）

```sh
PYTHONPATH=. python3 -m rvault --store $W/m6 mail receive --input $W/samples/safe.eml
```

期望：退出码 0，`safe-b1` accepted；工作目录递归搜索不到名为
`data.jsonl` / `manifest.json` 的新文件（发送方文件名只作元数据，
绝不决定宿主写入路径）。

### 3.5 部分接收（阶段 5）

```sh
PYTHONPATH=. python3 -m rvault --store $W/m3 mail receive --input $W/samples/partial.eml
```

期望（退出码 2）：`status: "partial"`；三批依次为
`accepted / rejected / accepted`；pb2 的 `errors` 恰为
`[{"line": 2, "reason": "field edition must be a positive integer"}]`；
输出不含 `SECRETNAME` / `SECRETMEMO`。库内只有 po1、po2、po6、po7
四条观测与 4 条 refresh 通知（2 帧）。再次 receive 同一文件：同样的
回执 + `note`（只回读记录，不重导）。

### 3.6 强退与续办（阶段 6）

演练用同步点 `mail.after_batch_import.1` 让 receive 进程在**批次 c1 已
提交、处理记录尚未更新**处暂停并 SIGKILL。此时直接查库可见：

- `batches` 表已有 `c1`（业务已提交），`editions` 有 `k1`；
- `ml_batches` 三行全是 `pending`（记录未更新）；
- `tmp/` 里留有崩溃进程的 `mail.<pid>.*.part`。

然后：

```sh
PYTHONPATH=. python3 -m rvault --store $W/m4 mail resume --input $W/samples/crash.eml
```

期望（退出码 0）：三批依次为 `duplicate / accepted / accepted`——c1 靠
原 batch_tag 凭据重新确认，**没有**重复观测或通知（`cf_events` 恰 3 条：
k1/k2/k3 各一条 refresh）；`tmp/` 里崩溃遗留文件被回收；消息状态
`done`。再次 `resume`：同样回执 + `note`，空操作。

### 3.7 撤回屏障（阶段 7）

```sh
PYTHONPATH=. python3 -m rvault --store $W/mw withdraw register \
    --withdrawal-id W1 --study mxw --person p-dan
PYTHONPATH=. python3 -m rvault --store $W/mw mail receive --input $W/samples/withdraw.eml
```

期望（退出码 0）：批次 `wd-b1` accepted，`accepted: 1`，
`withdrawn_suppressed: 1`；`query --study mxw` 只有 `site-omega`
（p-new，50）；`cf_events` 恰一条 `refresh mxw site-omega`——被抑制的行
不产生通知。邮件适配层复用既有 intake，屏障语义与 `import` 完全一致。

### 3.8 大邮件与内存（阶段 8）

```sh
PYTHONPATH=. python3 -m rvault --store $W/m5 mail receive --input $W/samples/big.eml
```

期望（退出码 0）：`bigb` accepted，120 000 条观测。演练在子进程里接收
并自报峰值内存：输入 20 703 423 字节（.eml）/ 15 128 890 字节（解码后），
峰值 RSS（VmHWM）约 2 万 kB 量级、远小于输入规模（演练断言 < 96 MB 并
打印实测值；本机实测约 19–20 MB）。

### 3.9 办理情况查询（阶段 9）

```sh
PYTHONPATH=. python3 -m rvault --store $W/m1 mail status                    # 列表
PYTHONPATH=. python3 -m rvault --store $W/m1 mail status --message <摘要>   # 单封
PYTHONPATH=. python3 -m rvault --store $W/m1 mail status --message 000...0  # 退出码 4
PYTHONPATH=. python3 -m rvault --store $W/m1 mail resume --input $W/samples/unsent.eml   # 退出码 4
```

期望：单封视图给出 `state`、清单位置、逐批 `batch_tag`/部分位置/状态；
输出只有标识符与类别，没有载荷；未知摘要与从未接收的邮件都返回退出码 4。

## 4. 独立校验器

演练在每个阶段边界调用（也可手动重跑）：

```sh
python3 demo/verify_mail.py --store $W/m1       --checkpoint main
python3 demo/verify_mail.py --store $W/m1s      --checkpoint main
python3 demo/verify_mail.py --store $W/errstore --checkpoint errors
python3 demo/verify_mail.py --store $W/m6       --checkpoint safe
python3 demo/verify_mail.py --store $W/m3       --checkpoint partial
python3 demo/verify_mail.py --store $W/m4       --checkpoint resumed
python3 demo/verify_mail.py --store $W/m5       --checkpoint big
```

校验器不经过 rvault 代码，用自己的 SQL 逐字段比对
`demo/expected_mail.py` 中的手算值；任一不符即非零退出。
