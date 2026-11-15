"""Finite writer, frozen expectations and raw resource observations; no export manager."""
import argparse
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import time


def call(python, app, *args):
    result = subprocess.run([python, str(Path(app).resolve())] + [str(x) for x in args],
                            capture_output=True, text=True, timeout=30)
    if result.returncode:
        raise ValueError(result.stderr)
    return json.loads(result.stdout)


def feed(root, app, python, start, batches, groups, interval, manual_run):
    root = Path(root).resolve()
    with tempfile.TemporaryDirectory(prefix="export-feed-") as temporary:
        path = Path(temporary) / "input.json"
        for number in range(start, start + batches):
            batch = {"batch": "load-%06d" % number, "segments": [[
                {"id": "x-%06d" % i, "rev": 1, "sensor": "s-%06d" % i,
                 "ts": number * 60, "value": i - 100, "deleted": False}
                for i in range(groups)]]}
            path.write_text(json.dumps(batch), encoding="utf-8")
            ack = call(python, app, "submit", root, path)
            deadline = time.monotonic() + 30
            while True:
                if manual_run:
                    call(python, app, "run", root, "--once")
                if any(item["batch"] == batch["batch"]
                       for item in call(python, app, "read", root)):
                    break
                if time.monotonic() > deadline:
                    raise ValueError("FEED_TIMEOUT")
                time.sleep(0.02)
            print(json.dumps({"accepted": ack, "completed": batch["batch"]}), flush=True)
            time.sleep(interval)


def observe(root, checkpoint=False):
    root = Path(root).resolve()
    output = {}
    if checkpoint:
        db = sqlite3.connect(str(root / "state.sqlite"), timeout=1)
        db.execute("PRAGMA busy_timeout=1000")
        try:
            output["checkpoint"] = list(db.execute(
                "PRAGMA wal_checkpoint(TRUNCATE)").fetchone())
        finally:
            db.close()
    wal = root / "state.sqlite-wal"
    output["wal_bytes"] = wal.stat().st_size if wal.exists() else 0
    directory = root / "exports"
    files = {}
    if directory.exists():
        for path in directory.rglob("*"):
            if path.is_file():
                files[str(path.relative_to(directory))] = path.stat().st_size
    output["snapshot_files"] = files
    output["snapshot_bytes"] = sum(files.values())
    return output


def freeze(root, out, app, python):
    result = call(python, app, "read", root)
    Path(out).write_text(json.dumps(result, ensure_ascii=False), encoding="utf-8")
    return {"expected": str(Path(out).resolve()), "batches": len(result)}


def check(expected, lines):
    want = json.loads(Path(expected).read_text(encoding="utf-8"))
    got = [json.loads(line) for line in Path(lines).read_text(encoding="utf-8").splitlines()]
    assert got == want, {"expected_batches": len(want), "actual_batches": len(got)}
    return {"matched_batches": len(got), "results": got}


def wait(gate, seconds):
    ready = Path(gate) / "ready.json"
    deadline = time.monotonic() + seconds
    while not ready.exists():
        if time.monotonic() >= deadline:
            raise ValueError("GATE_TIMEOUT")
        time.sleep(0.02)
    return json.loads(ready.read_text(encoding="utf-8"))


def main():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser("feed")
    p.add_argument("root")
    p.add_argument("--app", required=True)
    p.add_argument("--python", default=sys.executable)
    p.add_argument("--manual-run", action="store_true")
    p.add_argument("--start", type=int, default=0)
    p.add_argument("--batches", type=int, default=4)
    p.add_argument("--groups", type=int, choices=range(1, 2001), default=256)
    p.add_argument("--interval", type=float, default=0.05)
    p = sub.add_parser("observe")
    p.add_argument("root")
    p.add_argument("--checkpoint", action="store_true")
    p = sub.add_parser("freeze")
    p.add_argument("root")
    p.add_argument("out")
    p.add_argument("--app", required=True)
    p.add_argument("--python", default=sys.executable)
    p = sub.add_parser("check")
    p.add_argument("expected")
    p.add_argument("lines")
    for name in ("wait", "resume"):
        p = sub.add_parser(name)
        p.add_argument("gate")
        if name == "wait":
            p.add_argument("--seconds", type=float, default=20)
    args = parser.parse_args()
    if args.command == "feed":
        feed(args.root, args.app, args.python, args.start, args.batches,
             args.groups, args.interval, args.manual_run)
        return
    if args.command == "observe":
        value = observe(args.root, args.checkpoint)
    elif args.command == "freeze":
        value = freeze(args.root, args.out, args.app, args.python)
    elif args.command == "check":
        value = check(args.expected, args.lines)
    elif args.command == "wait":
        value = wait(args.gate, args.seconds)
    else:
        if not (Path(args.gate) / "ready.json").is_file():
            raise ValueError("GATE_NOT_READY")
        (Path(args.gate) / "continue").touch()
        value = {"resumed": str(Path(args.gate).resolve())}
    print(json.dumps(value, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
