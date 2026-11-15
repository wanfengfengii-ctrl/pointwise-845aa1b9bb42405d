"""Managed snapshot exports: admission, disk budget, liveness and reaping.

An export first pins a per-export flock (an empty lock file, outside the byte
budget).  The frozen snapshot is built inside a short read-only transaction and
persisted as content-addressed JSON Lines under ROOT/exports, so a slow consumer
never holds a production database transaction.  A small registry outside the
exports directory coordinates slots, reference counts and liveness; the resident
worker reaps exports whose lock is no longer held by a live process.
"""
import contextlib
import errno
import fcntl
import hashlib
import json
import os
from pathlib import Path
import signal
import sqlite3
import sys
import tempfile
import time
from urllib.parse import quote

from localio import Gate
from pipeline import encoded

DEFAULT_BUDGET = 64 * 1024 * 1024
MIN_BUDGET = 4096
MAX_BUDGET = 268435456
MAX_ACTIVE = 2
REAP_INTERVAL = 0.5
CAPACITY = "EXPORT_CAPACITY"


class ExportError(Exception):
    """Non-zero exit with an explanatory stderr message."""


def load_budget(root):
    path = Path(root) / "export-config.json"
    if not path.exists():
        return DEFAULT_BUDGET
    try:
        config = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ExportError("EXPORT_CONFIG: %s" % exc)
    budget = config.get("budget_bytes")
    if type(budget) is not int or not MIN_BUDGET <= budget <= MAX_BUDGET:
        raise ExportError("EXPORT_CONFIG: budget_bytes must be an integer in %d..%d"
                          % (MIN_BUDGET, MAX_BUDGET))
    return budget


def paths(root):
    root = Path(root).resolve()
    exports = root / "exports"
    return {
        "root": root,
        "exports": exports,
        "registry": root / "export-registry.json",
        "registry_lock": root / "export-registry.lock",
    }


def _empty_registry():
    return {"version": 1, "exports": {}, "snapshots": {}}


def load_registry(place):
    try:
        data = json.loads(place["registry"].read_text(encoding="utf-8"))
    except FileNotFoundError:
        return _empty_registry()
    except (OSError, ValueError) as exc:
        raise ExportError("EXPORT_REGISTRY: %s" % exc)
    if not isinstance(data, dict) or set(data) != {"version", "exports", "snapshots"}:
        raise ExportError("EXPORT_REGISTRY: bad shape")
    return data


def save_registry(place, registry):
    place["root"].mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=".registry-", dir=str(place["root"]))
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(encoded(registry))
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, str(place["registry"]))
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


@contextlib.contextmanager
def registry_guard(place):
    place["root"].mkdir(parents=True, exist_ok=True)
    with place["registry_lock"].open("a+b") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        yield load_registry(place)


def is_alive(lock_path):
    """A held flock means the owner process is alive (SIGSTOP keeps the lock)."""
    try:
        handle = open(str(lock_path), "a+b")
    except FileNotFoundError:
        return False
    try:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            if exc.errno in (errno.EACCES, errno.EAGAIN):
                return True
            raise
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        return False
    finally:
        handle.close()


def managed_bytes(place):
    """Sum of logical lengths of every file under ROOT/exports."""
    total = 0
    if place["exports"].exists():
        for path in place["exports"].rglob("*"):
            if path.is_file():
                total += path.stat().st_size
    return total


def snapshot_path(place, key):
    return place["exports"] / ("snapshot-%s.jsonl" % key)


def staging_path(place, export_id, key):
    return place["exports"] / (".staging-%s-%s.jsonl" % (export_id, key))


def lock_path(place, export_id):
    return place["exports"] / ("export-%s.lock" % export_id)


def _release_snapshot(place, registry, key):
    snapshot = registry["snapshots"].get(key)
    if snapshot is None:
        return
    snapshot["refs"] -= 1
    if snapshot["refs"] <= 0:
        target = snapshot_path(place, key)
        with contextlib.suppress(FileNotFoundError):
            target.unlink()
        del registry["snapshots"][key]


def reap(place):
    """Release dead exports, their snapshot refs and orphaned files.

    Safe to call from the resident worker or at startup.  Lock files of live
    owners (including SIGSTOPped ones) are never touched.
    """
    place["exports"].mkdir(parents=True, exist_ok=True)
    reaped = []
    with registry_guard(place) as registry:
        for export_id in list(registry["exports"]):
            entry = registry["exports"][export_id]
            if not entry.get("active"):
                continue
            if is_alive(lock_path(place, export_id)):
                continue
            key = entry.get("snapshot")
            if key is not None:
                _release_snapshot(place, registry, key)
            with contextlib.suppress(FileNotFoundError):
                lock_path(place, export_id).unlink()
            del registry["exports"][export_id]
            reaped.append(export_id)
        # Garbage-collect files no registry entry references.
        known = set()
        for key in registry["snapshots"]:
            known.add(snapshot_path(place, key).name)
        for entry in registry["exports"].values():
            staging = entry.get("staging")
            if staging:
                known.add(Path(staging).name)
        for path in place["exports"].iterdir():
            if path.is_file() and path.name not in known:
                # Never remove a lock a live exporter still holds.
                if path.name.startswith("export-") and path.name.endswith(".lock"):
                    export_id = path.name[len("export-"):-len(".lock")]
                    if is_alive(path):
                        continue
                with contextlib.suppress(FileNotFoundError):
                    path.unlink()
        save_registry(place, registry)
    return reaped


def _open_production(root):
    path = (Path(root).resolve() / "state.sqlite").as_posix()
    db = sqlite3.connect("file:" + quote(path, safe="/") + "?mode=ro",
                         uri=True, timeout=30, isolation_level=None)
    db.execute("PRAGMA busy_timeout=30000")
    return db


def _frozen_records(db):
    """Read-only snapshot; ordering and payloads match Engine.read exactly."""
    records = []
    exists = db.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='jobs'").fetchone()
    if not exists:
        return records
    for key, result, receipt in db.execute(
            "SELECT id,result,receipt FROM jobs "
            "WHERE result IS NOT NULL ORDER BY id"):
        records.append({"batch": key, "rows": json.loads(result),
                        "receipt": json.loads(receipt)})
    return records


def run_export(root, output=None, hook=None):
    output = output if output is not None else sys.stdout
    hook = hook or Gate()
    root = Path(root).resolve()
    place = paths(root)
    place["exports"].mkdir(parents=True, exist_ok=True)
    budget = load_budget(root)
    export_id = "%d-%d" % (os.getpid(), int(time.time() * 1_000_000))
    my_lock = lock_path(place, export_id)
    lock_handle = open(str(my_lock), "a+b")
    fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    state = {"released": False, "key": None, "staging": None}

    def release(registry):
        if state["released"]:
            return
        state["released"] = True
        entry = registry["exports"].get(export_id)
        if entry is not None:
            key = entry.get("snapshot")
            if key is not None:
                _release_snapshot(place, registry, key)
            staging = entry.get("staging")
            if staging:
                with contextlib.suppress(FileNotFoundError):
                    Path(staging).unlink()
            del registry["exports"][export_id]
            save_registry(place, registry)
        with contextlib.suppress(FileNotFoundError):
            my_lock.unlink()

    try:
        # 1. Admission: at most two active exports (building counts as active).
        with registry_guard(place) as registry:
            active = 0
            for eid, entry in registry["exports"].items():
                if entry.get("active") and is_alive(lock_path(place, eid)):
                    active += 1
            if active >= MAX_ACTIVE:
                raise ExportError(CAPACITY + ": no active export slot (limit %d)" % MAX_ACTIVE)
            registry["exports"][export_id] = {"active": True, "pid": os.getpid(),
                                              "started": time.time(),
                                              "lock": str(my_lock), "snapshot": None,
                                              "staging": None}
            save_registry(place, registry)

        # 2. Freeze the snapshot inside a short read-only transaction.
        db_path = root / "state.sqlite"
        db = _open_production(root) if db_path.exists() else None
        try:
            if db is not None:
                db.execute("BEGIN")
            try:
                records = _frozen_records(db) if db is not None else []
                hasher = hashlib.sha256()
                for record in records:
                    hasher.update(encoded(record))
                    hasher.update(b"\n")
                key = hasher.hexdigest()
                hook("export_opened", {"completed": len(records)})
                target = snapshot_path(place, key)
                staged_event = False
                with registry_guard(place) as registry:
                    existing = registry["snapshots"].get(key)
                    current_bytes = managed_bytes(place)
                    if existing is None and not target.exists():
                        if records and current_bytes >= budget:
                            raise ExportError(CAPACITY + ": snapshot needs bytes but budget %d is full" % budget)
                        if records:
                            staging = staging_path(place, export_id, key)
                            state["staging"] = str(staging)
                            registry["exports"][export_id]["staging"] = str(staging)
                            save_registry(place, registry)
                            written = 0
                            with open(str(staging), "wb") as stream:
                                for record in records:
                                    line = encoded(record) + b"\n"
                                    if current_bytes + written + len(line) > budget:
                                        raise ExportError(
                                            CAPACITY + ": snapshot exceeds budget %d bytes" % budget)
                                    stream.write(line)
                                    written += len(line)
                                    if not staged_event:
                                        stream.flush()
                                        os.fsync(stream.fileno())
                                        hook("export_staged", {"path": str(staging)})
                                        staged_event = True
                                stream.flush()
                                os.fsync(stream.fileno())
                            os.replace(str(staging), str(target))
                            state["staging"] = None
                            # Still inside the same registry lock; mutate the
                            # held object instead of reloading (which would
                            # discard concurrent updates made under this lock).
                            registry["exports"][export_id]["staging"] = None
                            registry["snapshots"][key] = {
                                "path": str(target), "bytes": target.stat().st_size,
                                "refs": 1, "batches": len(records)}
                            registry["exports"][export_id]["snapshot"] = key
                            save_registry(place, registry)
                    else:
                        # Share an identical frozen snapshot.
                        if not target.exists():
                            raise ExportError("EXPORT_REGISTRY: missing shared snapshot")
                        registry["snapshots"][key]["refs"] += 1
                        registry["exports"][export_id]["snapshot"] = key
                        save_registry(place, registry)
            finally:
                # The production read transaction ends before any consumer I/O.
                if db is not None:
                    db.rollback()
        finally:
            if db is not None:
                db.close()

        # 3. Stream the on-disk snapshot; backpressure here cannot touch the WAL.
        hook("export_streaming", {"completed": len(records)})
        if records:
            with open(str(target), "rb") as stream:
                for line in stream:
                    output.write(line.decode("utf-8"))
                    output.flush()
        with registry_guard(place) as registry:
            hook("export_releasing", {"completed": len(records)})
            release(registry)
    except BrokenPipeError:
        # Consumer closed the pipe: release quietly, exit non-zero only via signal.
        with contextlib.suppress(Exception):
            with registry_guard(place) as registry:
                hook("export_releasing", {"completed": -1})
                release(registry)
        with contextlib.suppress(Exception):
            output.close()
        os._exit(0)
    except BaseException:
        with contextlib.suppress(Exception):
            with registry_guard(place) as registry:
                hook("export_releasing", {"completed": -1})
                release(registry)
        raise
    else:
        return len(records)
    finally:
        with contextlib.suppress(Exception):
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)
        lock_handle.close()


def _terminate_on_signal(signum, frame):  # pragma: no cover - signal path
    raise KeyboardInterrupt("signal %d" % signum)


def export(root):
    signal.signal(signal.SIGTERM, _terminate_on_signal)
    run_export(root)


def status_exports(root):
    place = paths(root)
    budget = load_budget(root)
    active = 0
    if place["registry"].exists():
        with registry_guard(place) as registry:
            for eid, entry in registry["exports"].items():
                if entry.get("active") and is_alive(lock_path(place, eid)):
                    active += 1
    return {"active": active, "bytes": managed_bytes(place), "budget": budget}
