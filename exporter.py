"""Baseline stream: correct rows, but the consumer pins the production WAL."""
import json
from pathlib import Path
import sqlite3
import sys
from urllib.parse import quote
from localio import Gate
from pipeline import encoded


def stream(db, output, hook=None):
    hook = hook or (lambda event, detail: None)
    count = 0
    db.execute("BEGIN")
    try:
        count = db.execute(
            "SELECT count(*) FROM jobs WHERE result IS NOT NULL").fetchone()[0]
        hook("export_opened", {"completed": count})
        hook("export_streaming", {"completed": count})
        for key, result, receipt in db.execute(
                "SELECT id,result,receipt FROM jobs "
                "WHERE result IS NOT NULL ORDER BY id"):
            record = {"batch": key, "rows": json.loads(result),
                      "receipt": json.loads(receipt)}
            output.write(encoded(record).decode("utf-8") + "\n")
            output.flush()
    finally:
        try:
            hook("export_releasing", {"completed": count})
        finally:
            db.rollback()


def export(root):
    path = (Path(root).resolve() / "state.sqlite").as_posix()
    db = sqlite3.connect("file:" + quote(path, safe="/") + "?mode=ro",
                         uri=True, timeout=30, isolation_level=None)
    db.execute("PRAGMA busy_timeout=30000")
    try:
        stream(db, sys.stdout, Gate())
    finally:
        db.close()
