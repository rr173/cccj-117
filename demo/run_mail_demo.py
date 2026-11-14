#!/usr/bin/env python3
"""End-to-end demo for the offline .eml intake channel.

Builds REAL raw MIME byte streams (no pre-split attachments), then walks
through: a nested multipart message with two differently encoded data
attachments and a manifest; a deliberately tiny read-chunk run that forces
boundary lines, base64 quads and quoted-printable soft breaks to straddle
chunks; preflight error cases (digest mismatch, ambiguous Content-ID,
missing closing boundary, unsupported and invalid transfer encodings,
duplicate/corrupt manifests, unresolvable reference, nested message); a
traversal file name that must stay inert; a partial-receipt message; a
SIGKILL between a batch commit and its record update followed by a resume;
and a large-attachment message with a peak-memory measurement.

Everything the demo touches (stores, generated .eml samples, logs, sync
markers) lives in a fresh temporary directory printed at the start;
override with RVAULT_MAIL_WORK.  All expectations are hand-written in
demo/expected_mail.py; verify_mail.py compares the real database against
them with its own SQL.

Usage:  python3 demo/run_mail_demo.py
"""
from __future__ import annotations

import base64
import hashlib
import json
import os
import shutil
import signal
import sqlite3
import subprocess
import sys
import tempfile
import time

DEMO = os.path.dirname(os.path.abspath(__file__))
PROJ = os.path.dirname(DEMO)
WORK = os.environ.get("RVAULT_MAIL_WORK") or tempfile.mkdtemp(prefix="rvault-mail-")
IN = os.path.join(WORK, "samples")
LOGS = os.path.join(WORK, "logs")
SYNC = os.path.join(WORK, "sync")

sys.path.insert(0, DEMO)
import expected_mail as E  # noqa: E402

BASE_ENV = dict(os.environ)
BASE_ENV["PYTHONPATH"] = PROJ

_step = 0
_failures = []


def log(name, text):
    with open(os.path.join(LOGS, name), "a", encoding="utf-8") as f:
        f.write(text)


def rv(store, *args, ok=(0,), tag=None, env=None):
    global _step
    _step += 1
    cmd = [sys.executable, "-m", "rvault", "--store", store] + list(args)
    r = subprocess.run(cmd, capture_output=True, text=True, env=env or BASE_ENV)
    name = "%03d_%s" % (_step, tag or (args[0] if args else "cmd"))
    log(name + ".out", r.stdout)
    log(name + ".err", r.stderr)
    if r.returncode not in ok:
        raise SystemExit("command failed rc=%d: %s\nstdout:\n%s\nstderr:\n%s"
                         % (r.returncode, " ".join(cmd), r.stdout, r.stderr))
    return r


def rvj(store, *args, **kw):
    return json.loads(rv(store, *args, **kw).stdout)


def check(cond, msg):
    if cond:
        print("ok  ", msg)
    else:
        _failures.append(msg)
        print("FAIL", msg)


def verify(store, cp):
    r = subprocess.run([sys.executable, os.path.join(DEMO, "verify_mail.py"),
                        "--store", store, "--checkpoint", cp],
                       capture_output=True, text=True)
    log("verify_%s.log" % cp, r.stdout + r.stderr)
    if r.returncode != 0:
        print(r.stdout)
        print(r.stderr, file=sys.stderr)
        raise SystemExit("verify %s FAILED" % cp)
    print("== checkpoint %s ok" % cp)


def sql(store, q, args=()):
    conn = sqlite3.connect(os.path.join(store, "vault.sqlite3"))
    try:
        return list(conn.execute(q, args))
    finally:
        conn.close()


# --------------------------------------------------------------------------
# MIME assembly (real raw bytes)
# --------------------------------------------------------------------------
def H(pairs):
    return b"".join(k.encode("utf-8") + b": " + v.encode("utf-8") + b"\r\n"
                    for k, v in pairs) + b"\r\n"


def multipart(boundary, entities, preamble=b"", epilogue=b"", close=True):
    B = boundary.encode()
    out = preamble
    for e in entities:
        out += b"--" + B + b"\r\n" + e + b"\r\n"
    if close:
        out += b"--" + B + b"--\r\n" + epilogue
    return out


def b64_fold(data, width=76):
    enc = base64.b64encode(data)
    return b"\r\n".join(enc[i:i + width] for i in range(0, len(enc), width))


def qp_encode(data, width=72):
    """Quoted-printable with =XX escapes and soft line breaks."""
    out = bytearray()
    col = 0
    i = 0
    n = len(data)
    while i < n:
        b = data[i]
        if b == 0x0A:
            out.append(0x0A)
            col = 0
            i += 1
            continue
        nxt = data[i + 1] if i + 1 < n else None
        if 33 <= b <= 60 or 62 <= b <= 126:
            tok = bytes([b])
        elif b in (0x20, 0x09) and nxt not in (0x0A, None):
            tok = bytes([b])
        else:
            tok = b"=%02X" % b
        if col + len(tok) > width - 1:
            out += b"=\r\n"
            col = 0
        out += tok
        col += len(tok)
        i += 1
    return bytes(out)


def manifest_entity(entries, cte="8bit", filename=None, extra_headers=()):
    body = (json.dumps({"batches": [
        {"batch_tag": t, "content_id": c, "sha256": s}
        for t, c, s in entries]}) + "\n").encode("utf-8")
    headers = [("Content-Type", "application/vnd.rvault-batches+json; "
                                "charset=utf-8"),
               ("Content-Transfer-Encoding", cte),
               ("Content-ID", "<manifest@rvault>")]
    headers += list(extra_headers)
    if filename:
        headers.append(("Content-Disposition",
                        'attachment; filename="%s"' % filename))
    if cte == "base64":
        body = b64_fold(body)
    return H(headers) + body


def data_entity(payload, cte, cid, filename=None, ctype="application/x-ndjson",
                extra_headers=()):
    headers = [("Content-Type", ctype + '; name="data.jsonl"'),
               ("Content-Transfer-Encoding", cte),
               ("Content-ID", "<%s>" % cid)]
    headers += list(extra_headers)
    if filename:
        headers.append(("Content-Disposition",
                        'attachment; filename="%s"' % filename))
    if cte == "base64":
        body = b64_fold(payload)
    elif cte == "quoted-printable":
        body = qp_encode(payload)
    else:
        body = payload
    return H(headers) + body


def message(entities, boundary="mixBOUNDARY123", close=True, extra_ct=""):
    ct = 'multipart/mixed; boundary="%s"' % boundary
    headers = [("From", "Research Export <export@upstream.example>"),
               ("To", "intake@rvault.local"),
               ("Subject", "offline batch export"),
               ("Date", "Fri, 13 Nov 2026 09:00:00 +0000"),
               ("Message-ID", "<msg-%s@upstream.example>"
                              % hashlib.sha1(repr(entities).encode()
                                             ).hexdigest()[:12]),
               ("MIME-Version", "1.0"),
               ("Content-Type", ct + extra_ct)]
    return H(headers) + multipart(boundary, entities, close=close)


def write_sample(name, data):
    path = os.path.join(IN, name)
    with open(path, "wb") as f:
        f.write(data)
    return path


# --------------------------------------------------------------------------
# fixture builders
# --------------------------------------------------------------------------
def build_mail1():
    """Nested related image + base64 data + quoted-printable data + manifest."""
    text_body = (
        b"This mail carries two research batches for study mx1.\r\n"
        b"\r\n"
        b"The JSON block below is ONLY documentation inside the prose -\r\n"
        b"it is not a manifest and must never be treated as one:\r\n"
        b"\r\n"
        b'{"batches": [{"batch_tag": "fake-batch", "content_id": '
        b'"fake@example", "sha256": "' + b"0" * 64 + b'"}]}' b"\r\n"
        b"\r\n"
        b"The following lines merely share a prefix with real boundaries;\r\n"
        b"they are body text, not delimiters:\r\n"
        b"--mixBOUNDARY1234-extended-prefix-is-not-a-delimiter\r\n"
        b"--mixBOUNDARY12\r\n"
        b"--relBOUNDARY456\r\n")
    part_text = H([("Content-Type", "text/plain; charset=utf-8"),
                   ("Content-Transfer-Encoding", "8bit")]) + text_body

    html_body = (b"<html><body><p>Chart for this export:</p>"
                 b'<img src="cid:img1@rvault"></body></html>')
    part_html = H([("Content-Type", "text/html; charset=utf-8"),
                   ("Content-Transfer-Encoding", "8bit"),
                   ("Content-ID", "<html1@rvault>")]) + html_body
    png = b"\x89PNG\r\n\x1a\n" + bytes(range(64))
    part_img = H([("Content-Type", "image/png"),
                  ("Content-Transfer-Encoding", "base64"),
                  ("Content-ID", "<img1@rvault>"),
                  ("Content-Disposition", 'attachment; filename="chart.png"')]
                 ) + b64_fold(png)
    related = H([("Content-Type", 'multipart/related; boundary="relBOUNDARY456"; '
                                  'start="<html1@rvault>"')]) + \
        multipart("relBOUNDARY456", [part_html, part_img])

    part_data1 = data_entity(E.B1_PAYLOAD, "base64", "data-1@rvault",
                             filename="batch-one.jsonl")
    part_data2 = data_entity(E.B2_PAYLOAD, "quoted-printable", "data-2@rvault",
                             filename="batch-two.jsonl")
    part_manifest = manifest_entity(E.M1_BATCHES)
    return message([part_text, related, part_data1, part_data2, part_manifest])


def build_error_mails():
    """One .eml per preflight error case; returns {case_name: path}."""
    secret = E.ERR_SECRET
    payload = ('{"study":"e","person":"p","observation":"o","edition":1,'
               '"display_name":"%s","measure":1,"memo":""}\n'
               % secret).encode("utf-8")
    payload2 = ('{"study":"e","person":"p2","observation":"o2","edition":1,'
                '"display_name":"%s-2","measure":2,"memo":""}\n'
                % secret).encode("utf-8")
    h = hashlib.sha256(payload).hexdigest()
    h2 = hashlib.sha256(payload2).hexdigest()
    mails = {}

    # e1: manifest sha256 does not match the decoded attachment
    mails["e1_digest_mismatch"] = message([
        data_entity(payload, "7bit", "d@x"),
        manifest_entity([("t1", "d@x", "0" * 64)])])

    # e2: two parts share one Content-ID; the manifest references it
    mails["e2_duplicate_cid"] = message([
        data_entity(payload, "7bit", "dup@x"),
        data_entity(payload2, "7bit", "dup@x"),
        manifest_entity([("t1", "dup@x", h)])])

    # e3: the closing boundary delimiter is missing
    mails["e3_unterminated"] = message([
        data_entity(payload, "7bit", "d@x"),
        manifest_entity([("t1", "d@x", h)])], close=False)

    # e4: declared transfer encoding is not supported
    mails["e4_unsupported_cte"] = message([
        data_entity(payload, "x-uuencode", "d@x"),
        manifest_entity([("t1", "d@x", h)])])

    # e5: two data parts, each with malformed encoded content
    bad_b64 = H([("Content-Type", "application/x-ndjson"),
                 ("Content-Transfer-Encoding", "base64"),
                 ("Content-ID", "<d1@x>")]) + b"QUJD\r\n!!!!\r\nRA=="
    bad_qp = H([("Content-Type", "application/x-ndjson"),
                ("Content-Transfer-Encoding", "quoted-printable"),
                ("Content-ID", "<d2@x>")]) + b"abc=ZZdef"
    mails["e5_invalid_encoding"] = message([
        bad_b64, bad_qp,
        manifest_entity([("t1", "d1@x", h), ("t2", "d2@x", h2)])])

    # e7: a nested message/rfc822 carries a full manifest+data of its own;
    # the outer message has no manifest and must not pick up the inner one
    inner = message([data_entity(payload, "7bit", "inner@x"),
                     manifest_entity([("inner-t", "inner@x", h)])],
                    boundary="innerBOUNDARY")
    part_rfc822 = H([("Content-Type", "message/rfc822"),
                     ("Content-Transfer-Encoding", "8bit"),
                     ("Content-ID", "<fwd@x>")]) + inner
    mails["e7_rfc822"] = message([
        H([("Content-Type", "text/plain")]) + b"Forwarded for your info.\r\n",
        part_rfc822])

    # e8: two manifest parts
    mails["e8_manifest_duplicate"] = message([
        data_entity(payload, "7bit", "d@x"),
        manifest_entity([("t1", "d@x", h)]),
        H([("Content-Type", "application/vnd.rvault-batches+json"),
           ("Content-ID", "<manifest2@x>")]) +
        (json.dumps({"batches": []}) + "\n").encode()])

    # e9: manifest part is not valid JSON
    mails["e9_manifest_invalid"] = message([
        data_entity(payload, "7bit", "d@x"),
        H([("Content-Type", "application/vnd.rvault-batches+json"),
           ("Content-ID", "<manifest@x>")]) + b"{not json\r\n"])

    # e10: the same batch_tag is listed for two different payloads
    mails["e10_batch_tag_duplicate"] = message([
        data_entity(payload, "7bit", "d1@x"),
        data_entity(payload2, "7bit", "d2@x"),
        manifest_entity([("t1", "d1@x", h), ("t1", "d2@x", h2)])])

    # e11: the manifest references a Content-ID that does not exist
    mails["e11_reference_unresolved"] = message([
        data_entity(payload, "7bit", "d@x"),
        manifest_entity([("t1", "ghost@x", h)])])

    return {name: write_sample(name + ".eml", body)
            for name, body in mails.items()}


def build_partial_mail():
    return message([
        data_entity(E.PB1_PAYLOAD, "7bit", "pd-1@rvault"),
        data_entity(E.PB2_PAYLOAD, "quoted-printable", "pd-2@rvault"),
        data_entity(E.PB3_PAYLOAD, "base64", "pd-3@rvault"),
        manifest_entity(E.PARTIAL_BATCHES)])


def build_crash_mail():
    return message([
        data_entity(E.C1_PAYLOAD, "binary", "cd-1@rvault"),
        data_entity(E.C2_PAYLOAD, "base64", "cd-2@rvault"),
        data_entity(E.C3_PAYLOAD, "quoted-printable", "cd-3@rvault"),
        manifest_entity(E.CRASH_BATCHES)])


def build_safe_mail():
    return message([
        data_entity(E.SAFE_PAYLOAD, "base64", "sd-1@rvault",
                    filename="../../data.jsonl"),
        manifest_entity(E.SAFE_BATCHES, filename="../../manifest.json")])


def build_withdraw_mail():
    rows = [
        {"study": "mxw", "person": "p-dan", "observation": "site-delta",
         "edition": 1, "display_name": "Dan", "measure": 41, "memo": ""},
        {"study": "mxw", "person": "p-new", "observation": "site-omega",
         "edition": 1, "display_name": "New", "measure": 50, "memo": ""},
    ]
    payload = E.jl(rows, "\n", trailing=True)
    sha = hashlib.sha256(payload).hexdigest()
    return message([
        data_entity(payload, "base64", "wd-1@rvault"),
        manifest_entity([("wd-b1", "wd-1@rvault", sha)])])


def build_big_mail():
    rows = []
    for i in range(E.BIG_N):
        rows.append({"study": E.BIG_STUDY, "person": "pb",
                     "observation": "o%06d" % i, "edition": 1,
                     "display_name": "Pn", "measure": i, "memo": "m"})
    payload = E.jl(rows, "\n", trailing=True)
    sha = hashlib.sha256(payload).hexdigest()
    body = message([
        data_entity(payload, "base64", "big-1@rvault"),
        manifest_entity([("bigb", "big-1@rvault", sha)])])
    return body, len(payload)


# --------------------------------------------------------------------------
# phases
# --------------------------------------------------------------------------
def phase_main(m1):
    print("-- phase 1: nested message, two encodings, real MIME bytes")
    store = os.path.join(WORK, "m1")
    r = rvj(store, "mail", "receive", "--input", m1)
    check(r["status"] == "received", "mail 1 received")
    check([b["status"] for b in r["batches"]] == ["accepted", "accepted"],
          "both batches accepted")
    check([b["part"] for b in r["batches"]] == ["3", "4"],
          "manifest resolved Content-IDs to parts 3 and 4 (not file order)")
    check(r["manifest_part"] == "5" and r["part_count"] == 7,
          "manifest is part 5 of 7 (prose, related container, html, png, "
          "2 data parts, manifest)")
    check(r["batches"][0]["sha256"] == E.B1_SHA
          and r["batches"][1]["sha256"] == E.B2_SHA,
          "decoded byte digests match the hand-computed payload digests")
    check(r["batches"][0]["accepted"] == 3 and r["batches"][1]["accepted"] == 2,
          "line counts: 3 + 2 observations accepted")
    tmp = os.path.join(store, "tmp")
    check(os.listdir(tmp) == [], "managed temp area cleaned after receive")
    verify(store, "main")
    return store


def phase_small_chunk(m1, store_big):
    print("-- phase 2: the same message with a 7-byte read chunk")
    store = os.path.join(WORK, "m1s")
    r = rvj(store, "mail", "receive", "--input", m1, "--chunk-size", "7")
    check(r["status"] == "received",
          "mail 1 received with chunk size 7 (boundaries, base64 quads and "
          "soft breaks straddle chunks)")
    verify(store, "main")
    for q, what in (
            ("SELECT batch_tag, digest FROM batches ORDER BY 1",
             "batch digests"),
            ("SELECT study, observation, edition, person, display_name, "
             "measure, memo, batch_tag FROM editions ORDER BY 1,2,3",
             "editions"),
            ("SELECT seq, op, study, observation FROM cf_events ORDER BY 1",
             "change-feed events"),
            ("SELECT seq, batch_tag, part, sha256, status FROM ml_batches "
             "ORDER BY 1", "mail batch records")):
        check(sql(store_big, q) == sql(store, q),
              "chunk size 7 == chunk size 65536: %s identical" % what)
    return store


def phase_errors(mails):
    print("-- phase 3: preflight error cases (nothing may be imported)")
    store = os.path.join(WORK, "errstore")
    digests = {}
    for name in sorted(mails):
        r = rvj(store, "mail", "receive", "--input", mails[name],
                ok=(2,), tag=name)
        check(r["status"] == "rejected", "%s rejected" % name)
        got = [e["category"] for e in r["errors"]]
        check(got == E.ERROR_CASES[name],
              "%s categories %s" % (name, got))
        if name in E.ERROR_PARTS:
            check(r["errors"][0].get("part") == E.ERROR_PARTS[name],
                  "%s reports part %s" % (name, E.ERROR_PARTS[name]))
        out = json.dumps(r, ensure_ascii=False)
        check(E.ERR_SECRET not in out,
              "%s diagnostics carry no payload values" % name)
        digests[name] = r["message_digest"]
        check(os.listdir(os.path.join(store, "tmp")) == [],
              "%s leaves no temp files" % name)
    with open(os.path.join(WORK, "error_cases.json"), "w",
              encoding="utf-8") as f:
        json.dump(digests, f)
    # re-receiving a rejected message re-runs preflight and stays rejected
    r = rvj(store, "mail", "receive", "--input",
            mails["e1_digest_mismatch"], ok=(2,), tag="e1_again")
    check(r["status"] == "rejected"
          and [e["category"] for e in r["errors"]] == ["digest_mismatch"],
          "rejected message re-received: same outcome, still nothing imported")
    verify(store, "errors")
    return store


def phase_safe(safe_mail):
    print("-- phase 4: traversal file names stay inert metadata")
    store = os.path.join(WORK, "m6")
    before = set()
    for root, dirs, files in os.walk(WORK):
        for f in files:
            before.add(os.path.join(root, f))
    r = rvj(store, "mail", "receive", "--input", safe_mail)
    check(r["status"] == "received" and r["batches"][0]["status"] == "accepted",
          "message with ../../data.jsonl file name received normally")
    stray = []
    for root, dirs, files in os.walk(WORK):
        for f in files:
            p = os.path.join(root, f)
            if p not in before and f in ("data.jsonl", "manifest.json"):
                stray.append(p)
    check(stray == [], "no host file was written under the sender's name")
    check(os.listdir(os.path.join(store, "tmp")) == [],
          "managed temp area cleaned")
    verify(store, "safe")


def phase_partial(pmail):
    print("-- phase 5: first batch legal, second has a bad line, third legal")
    store = os.path.join(WORK, "m3")
    r = rvj(store, "mail", "receive", "--input", pmail, ok=(2,))
    check(r["status"] == "partial", "message-level result is 'partial'")
    check([b["status"] for b in r["batches"]] == E.PARTIAL_STATUSES,
          "per-batch receipt: accepted / rejected / accepted")
    bad = r["batches"][1]
    check(bad["errors"] == [{"line": 2,
                             "reason": "field edition must be a positive "
                                       "integer"}],
          "rejected batch reports the bad line and category")
    out = json.dumps(r, ensure_ascii=False)
    check(E.P_SECRET_NAME not in out and E.P_SECRET_MEMO not in out,
          "rejected-batch diagnostics carry no display_name/memo values")
    verify(store, "partial")
    # re-processing the finished message replays the recorded outcome
    r2 = rvj(store, "mail", "receive", "--input", pmail, ok=(2,),
             tag="partial_again")
    check([b["status"] for b in r2["batches"]] == E.PARTIAL_STATUSES
          and "note" in r2,
          "re-received partial message replays the record, no re-import")
    verify(store, "partial")
    return store


def phase_crash(cmail):
    print("-- phase 6: SIGKILL after a batch commit, before the record update")
    store = os.path.join(WORK, "m4")
    os.makedirs(SYNC, exist_ok=True)
    env = dict(BASE_ENV)
    env["RVAULT_SYNC_DIR"] = SYNC
    env["RVAULT_SYNC_WAIT"] = "mail.after_batch_import.1"
    p = subprocess.Popen([sys.executable, "-m", "rvault", "--store", store,
                          "mail", "receive", "--input", cmail],
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                         text=True, env=env)
    deadline = time.time() + 60
    while not os.path.exists(os.path.join(
            SYNC, "mail.after_batch_import.1.reached")):
        if time.time() > deadline:
            p.kill()
            raise SystemExit("sync point never reached")
        time.sleep(0.02)
    p.send_signal(signal.SIGKILL)
    p.wait()
    log("crash_receive.out", p.stdout.read())
    log("crash_receive.err", p.stderr.read())
    check(p.returncode == -signal.SIGKILL, "receive process was SIGKILLed "
          "between the batch commit and the record update")
    verify(store, "crashed")
    leftovers = [f for f in os.listdir(os.path.join(store, "tmp"))
                 if f.startswith("mail.")]
    check(len(leftovers) > 0,
          "crashed run left decoded temp files behind (%s)" % leftovers)

    r = rvj(store, "mail", "resume", "--input", cmail)
    check(r["status"] == "received", "resume completed the message")
    check([b["status"] for b in r["batches"]] == E.RESUMED_STATUSES,
          "c1 re-confirmed via its batch credential (duplicate), "
          "c2/c3 accepted")
    check(os.listdir(os.path.join(store, "tmp")) == [],
          "resume reclaimed the crashed run's temp files")
    verify(store, "resumed")
    # resuming a fully processed message is a no-op replay
    r2 = rvj(store, "mail", "resume", "--input", cmail, tag="resume_done")
    check([b["status"] for b in r2["batches"]] == E.RESUMED_STATUSES
          and "note" in r2,
          "resuming a done message replays the record without re-import")
    verify(store, "resumed")
    return store


def phase_withdraw(wdmail):
    print("-- phase 7: mail intake rides the existing withdrawal barrier")
    store = os.path.join(WORK, "mw")
    rvj(store, "withdraw", "register", "--withdrawal-id", "W1",
        "--study", "mxw", "--person", "p-dan")
    r = rvj(store, "mail", "receive", "--input", wdmail)
    b = r["batches"][0]
    check(b["status"] == "accepted" and b["accepted"] == 1
          and b["withdrawn_suppressed"] == 1,
          "batch accepted with one line suppressed by the barrier")
    q = rvj(store, "query", "--study", "mxw")
    got = [(o["observation"], o["person"], o["measure"])
           for o in q["observations"]]
    check(got == [("site-omega", "p-new", 50)],
          "projection shows only the non-withdrawn observation")
    ev = sql(store, "SELECT op, study, observation FROM cf_events")
    check(ev == [("refresh", "mxw", "site-omega")],
          "suppressed line produced no notification; accepted one did")
    rows = sql(store, "SELECT status FROM ml_batches")
    check(rows == [("accepted",)],
          "mail record stores the existing intake outcome verbatim")


def phase_big(big_mail, decoded_size):
    print("-- phase 8: large attachment, input size vs peak memory")
    store = os.path.join(WORK, "m5")
    # The child reports its own peak RSS.  ru_maxrss is inherited across
    # fork on this platform (the demo parent is large after building the
    # sample), so the authoritative figure is VmHWM from /proc/self/status;
    # ru_maxrss is reported alongside for reference.
    code = r"""
import json, resource, sys
sys.argv = ["rvault", "--store", sys.argv[1], "mail", "receive",
            "--input", sys.argv[2]]
from rvault.cli import main
rc = main()
vmhwm = None
try:
    with open("/proc/self/status") as f:
        for line in f:
            if line.startswith("VmHWM:"):
                vmhwm = int(line.split()[1])
except OSError:
    pass
sys.stderr.write("\n" + json.dumps(
    {"maxrss_kb": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
     "vmhwm_kb": vmhwm}))
sys.exit(rc)
"""
    r = subprocess.run([sys.executable, "-c", code, store, big_mail],
                       capture_output=True, text=True, env=BASE_ENV)
    log("big_receive.out", r.stdout)
    log("big_receive.err", r.stderr)
    if r.returncode != 0:
        raise SystemExit("big receive failed rc=%d\n%s" % (r.returncode,
                                                           r.stderr))
    summary = json.loads(r.stdout)
    report = json.loads(r.stderr.strip().splitlines()[-1])
    eml_size = os.path.getsize(big_mail)
    check(summary["status"] == "received"
          and summary["batches"][0]["status"] == "accepted",
          "big message received")
    check(summary["batches"][0]["accepted"] == E.BIG_N,
          "%d observations accepted" % E.BIG_N)
    peak = report["vmhwm_kb"] or report["maxrss_kb"]
    print("    input: %d bytes (.eml), %d bytes decoded payload, "
          "%d rows" % (eml_size, decoded_size, E.BIG_N))
    print("    peak RSS of the receive process: %d kB (VmHWM; "
          "getrusage reports %d kB, inherited from the demo parent)"
          % (peak, report["maxrss_kb"]))
    check(peak < 96 * 1024,
          "peak RSS stays flat and far below the input size "
          "(%d kB for a %.1f MB message)"
          % (peak, eml_size / 1024.0 / 1024.0))
    verify(store, "big")
    return peak


def phase_status(m1, unsent, stores):
    print("-- phase 9: operator status views")
    digest = sql(stores["m1"], "SELECT message_digest FROM ml_messages")[0][0]
    r = rvj(stores["m1"], "mail", "status", "--message", digest)
    check(r["message_digest"] == digest and r["state"] == "done",
          "mail status --message shows the record")
    check([b["batch_tag"] for b in r["batches"]] == ["mail-b1", "mail-b2"],
          "status lists the real batch_tags")
    out = json.dumps(r, ensure_ascii=False)
    check("Müller" not in out and "café" not in out,
          "status output carries identifiers only, no payload")
    r = rvj(stores["m1"], "mail", "status")
    check(len(r["messages"]) == 1
          and r["messages"][0]["state"] == "done",
          "mail status lists all messages")
    r = rv(stores["m1"], "mail", "status", "--message", "0" * 64, ok=(4,))
    check(json.loads(r.stderr)["error"] == "unknown message",
          "unknown digest -> exit 4")
    r = rv(stores["m1"], "mail", "resume", "--input", unsent, ok=(4,),
           tag="resume_unknown")
    check(json.loads(r.stdout)["error"].startswith("unknown message"),
          "resume of a never-received message -> exit 4")


def main():
    os.makedirs(IN, exist_ok=True)
    os.makedirs(LOGS, exist_ok=True)
    print("mail demo work dir: %s" % WORK)

    print("-- phase 0: build real MIME samples")
    m1 = write_sample("mail1.eml", build_mail1())
    error_mails = build_error_mails()
    pmail = write_sample("partial.eml", build_partial_mail())
    cmail = write_sample("crash.eml", build_crash_mail())
    smail = write_sample("safe.eml", build_safe_mail())
    wdmail = write_sample("withdraw.eml", build_withdraw_mail())
    big_body, decoded_size = build_big_mail()
    big_mail = write_sample("big.eml", big_body)
    unsent = write_sample("unsent.eml", message([
        data_entity(E.SAFE_PAYLOAD, "7bit", "ud-1@rvault"),
        manifest_entity([("unsent-b1", "ud-1@rvault",
                          hashlib.sha256(E.SAFE_PAYLOAD).hexdigest())])]))
    print("    samples in %s (mail1: %d bytes, big: %d bytes)"
          % (IN, os.path.getsize(m1), os.path.getsize(big_mail)))

    stores = {}
    stores["m1"] = phase_main(m1)
    stores["m1s"] = phase_small_chunk(m1, stores["m1"])
    phase_errors(error_mails)
    phase_safe(smail)
    phase_partial(pmail)
    phase_crash(cmail)
    phase_withdraw(wdmail)
    rss = phase_big(big_mail, decoded_size)
    phase_status(m1, unsent, stores)

    if _failures:
        print("\nMAIL DEMO FAILED: %d checks failed" % len(_failures))
        for f in _failures:
            print("  - " + f)
        return 1
    print("\nMAIL DEMO COMPLETE  (big-message receive peak RSS: %d kB)" % rss)
    print("work dir: %s" % WORK)
    return 0


if __name__ == "__main__":
    sys.exit(main())
