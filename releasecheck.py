"""Installed business and full slow-export lifecycle regression.

The original baseline assertions (reproducible build, bad package rejection,
detached source, worker exclusion, fixture accounting, installed export) are
kept verbatim.  New sections cover two slow exports with continuous ingest and
WAL truncation, slot and budget rejections before any output, SIGSTOP liveness,
broken pipe, termination at every export gate, and service-restart reclamation.
A real user LaunchAgent run is attempted only on macOS with a GUI domain;
otherwise it is recorded as not executed rather than faked.
"""
import contextlib
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from agentcheck import inspect
from checks import assert_results, fixtures


def verify(demo=False):
    base = Path(__file__).resolve().parent
    python = str(Path(sys.executable).resolve())
    env = dict(os.environ, PYTHONNOUSERSITE="1", PYTHONDONTWRITEBYTECODE="1")
    for key in ("PYTHONPATH", "PYTHONHOME", "POINTWISE_PAUSE", "POINTWISE_GATE"):
        env.pop(key, None)
    evidence = {}
    children = []
    with tempfile.TemporaryDirectory(prefix="慢导出 release ") as temporary:
        area = Path(temporary).resolve()
        source, package, dest = area / "source", area / "package", area / "安装目录"
        source.mkdir()
        for path in base.glob("*.py"):
            shutil.copy2(path, source / path.name)
        work = area / "empty"
        work.mkdir()

        def call(argv, success=True, extra_env=None, timeout=60):
            result = subprocess.run(argv, cwd=str(work), env=extra_env or env,
                                    capture_output=True, text=True, timeout=timeout)
            if not success:
                assert result.returncode != 0, result.stdout
                return result
            assert result.returncode == 0, (argv, result.stdout, result.stderr)
            return json.loads(result.stdout)

        call([python, str(source / "ops.py"), "build", str(package)])
        second = area / "second"
        call([python, str(source / "ops.py"), "build", str(second)])
        assert (package / "app.pyz").read_bytes() == (second / "app.pyz").read_bytes()
        evidence["reproducible_build"] = True
        info = call([python, str(source / "ops.py"), "install", str(package), str(dest),
                     "--python", python, "--no-load"])
        app, root = Path(info["app"]), Path(info["root"])
        assert app == dest / "app.pyz" and root == dest / "data"
        assert inspect(info["plist"], app, root, python) == info["label"]
        shutil.rmtree(source)

        def command(*args):
            return [python, str(app)] + [str(arg) for arg in args]

        def run(*args):
            return call(command(*args))

        def submit(batch):
            path = area / (batch["batch"] + ".json")
            path.write_text(json.dumps(batch), encoding="utf-8")
            return run("submit", root, path)

        def finish():
            for _ in range(128):
                if not run("status", root)["pending"]:
                    return
                run("run", root, "--once")
            raise AssertionError("fixture did not complete")

        def big_batch(number, groups=256):
            return {"batch": "load-%06d" % number, "segments": [[
                {"id": "x-%06d" % i, "rev": 1, "sensor": "s-%06d" % i,
                 "ts": number * 60, "value": i - 100, "deleted": False}
                for i in range(groups)]]}

        def submit_drain(batch):
            path = area / (batch["batch"] + ".json")
            path.write_text(json.dumps(batch), encoding="utf-8")
            run("submit", root, path)
            for _ in range(256):
                if not run("status", root)["pending"]:
                    return
                run("run", root, "--once")
            raise AssertionError("batch did not complete: " + batch["batch"])

        def wait_gate(gate, seconds=30):
            ready = Path(gate) / "ready.json"
            deadline = time.monotonic() + seconds
            while not ready.exists():
                if time.monotonic() >= deadline:
                    raise AssertionError("GATE_TIMEOUT %s" % gate)
                time.sleep(0.02)
            return json.loads(ready.read_text(encoding="utf-8"))

        def start_paused_export(gate, out_path, event="export_streaming"):
            child_env = dict(env, POINTWISE_PAUSE=event, POINTWISE_GATE=str(gate))
            stream = open(str(out_path), "wb")
            child = subprocess.Popen(command("export", root), cwd=str(work), env=child_env,
                                     stdout=stream, stderr=subprocess.PIPE)
            children.append(child)
            return child, stream

        def wait_clean(seconds=10):
            deadline = time.monotonic() + seconds
            last = None
            while time.monotonic() < deadline:
                last = run("status", root)["exports"]
                leftovers = list((root / "exports").glob("*")) if (root / "exports").exists() else []
                if last["active"] == 0 and last["bytes"] == 0 and not leftovers:
                    return last
                time.sleep(0.2)
            raise AssertionError("export resources not reclaimed: %s" % last)

        try:
            # ---- Original baseline section, assertions unchanged ----
            before = app.read_bytes(), Path(info["plist"]).read_bytes()
            broken = area / "broken"
            shutil.copytree(package, broken)
            with (broken / "app.pyz").open("ab") as out:
                out.write(b"corrupt")
            call(command("install", broken, dest, "--python", python, "--no-load"), False)
            assert before == (app.read_bytes(), Path(info["plist"]).read_bytes())
            a, b = fixtures()
            ack = submit(a)
            submit(b)
            gate = area / "gate"
            child_env = dict(env, POINTWISE_PAUSE="checkpoint_loaded", POINTWISE_GATE=str(gate))
            child = subprocess.Popen(command("run", root, "--once"), cwd=str(work), env=child_env,
                                     stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            children.append(child)
            deadline = time.monotonic() + 20
            while not (gate / "ready.json").exists():
                assert child.poll() is None, child.communicate()
                if time.monotonic() > deadline:
                    raise AssertionError("worker did not reach checkpoint_loaded")
                time.sleep(0.02)
            call(command("run", root, "--once"), False)
            assert run("read", root) == []
            assert submit(a) == ack
            (gate / "continue").touch()
            stdout, stderr = child.communicate(timeout=30)
            assert child.returncode == 0, (stdout, stderr)
            finish()
            result = run("read", root)
            assert_results(result)
            exported = subprocess.run(command("export", root), cwd=str(work), env=env,
                                      capture_output=True, text=True, timeout=30)
            assert exported.returncode == 0, exported.stderr
            assert [json.loads(line) for line in exported.stdout.splitlines()] == result
            assert submit(a) == ack
            assert run("run", root, "--once") == {"progressed": False}
            evidence["baseline"] = {"results": result, "acceptance": ack,
                                    "reproducible": True, "detached_source": True}

            # ---- Empty export on an empty root stays empty ----
            empty_root = area / "empty-root"
            empty_root.mkdir()
            empty_run = subprocess.run(command("export", empty_root), cwd=str(work),
                                       env=env, capture_output=True, text=True, timeout=30)
            assert empty_run.returncode == 0 and empty_run.stdout == "", empty_run.stderr

            # ---- Capacity: budget rejection before any stdout byte ----
            submit_drain(big_batch(0))
            frozen_one = run("read", root)
            snap_probe = subprocess.run(command("export", root), cwd=str(work), env=env,
                                        capture_output=True, text=True, timeout=30)
            assert snap_probe.returncode == 0 and len(snap_probe.stdout.encode()) > 4096
            snapshot_bytes = len(snap_probe.stdout.encode())
            config = root / "export-config.json"
            config.write_text(json.dumps({"budget_bytes": 4096}), encoding="utf-8")
            refused = subprocess.run(command("export", root), cwd=str(work), env=env,
                                     capture_output=True, text=True, timeout=30)
            assert refused.returncode != 0 and refused.stdout == ""
            assert "EXPORT_CAPACITY" in refused.stderr, refused.stderr
            evidence["budget_rejection"] = {"stderr": refused.stderr.strip(),
                                            "snapshot_bytes": snapshot_bytes,
                                            "stdout_empty": True}
            config.unlink()

            # ---- Two slow exports; shared snapshot; slots then exhausted ----
            gate1, gate2 = area / "gate1", area / "gate2"
            out1, out2 = area / "out1.jsonl", area / "out2.jsonl"
            p1, f1 = start_paused_export(gate1, out1)
            ready1 = wait_gate(gate1)
            assert ready1["event"] == "export_streaming"
            assert ready1["detail"]["completed"] == len(frozen_one)
            p2, f2 = start_paused_export(gate2, out2)
            ready2 = wait_gate(gate2)
            assert ready2["detail"]["completed"] == len(frozen_one)
            slot_refused = subprocess.run(command("export", root), cwd=str(work), env=env,
                                          capture_output=True, text=True, timeout=30)
            assert slot_refused.returncode != 0 and slot_refused.stdout == ""
            assert "EXPORT_CAPACITY" in slot_refused.stderr
            evidence["slot_rejection"] = {"stderr": slot_refused.stderr.strip(),
                                          "stdout_empty": True}
            snapshot_files = list((root / "exports").glob("snapshot-*.jsonl"))
            assert len(snapshot_files) == 1, snapshot_files
            status_paused = run("status", root)
            assert status_paused["exports"]["active"] == 2
            assert status_paused["exports"]["bytes"] == snapshot_files[0].stat().st_size
            assert status_paused["exports"]["budget"] == 64 * 1024 * 1024
            evidence["paused_exports"] = status_paused["exports"]

            # ---- Resident worker keeps ingesting while readers stay paused ----
            worker = subprocess.Popen(command("run", root), cwd=str(work), env=env,
                                      stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            children.append(worker)
            time.sleep(1.0)
            for number in range(1, 5):
                batch = big_batch(number)
                path = area / (batch["batch"] + ".json")
                path.write_text(json.dumps(batch), encoding="utf-8")
                run("submit", root, path)
            deadline = time.monotonic() + 40
            while len(run("read", root)) < len(frozen_one) + 4:
                assert worker.poll() is None, worker.communicate()
                if time.monotonic() > deadline:
                    raise AssertionError("resident worker fell behind")
                time.sleep(0.1)

            # observe --checkpoint: truncate succeeds despite two paused readers
            observed = call([python, str(base / "exportlab.py"), "observe", root,
                             "--checkpoint"])
            assert observed["checkpoint"][0] == 0, observed
            assert observed["wal_bytes"] <= 65536, observed
            assert observed["snapshot_bytes"] == status_paused["exports"]["bytes"], observed
            evidence["wal_checkpoint"] = observed

            # ---- SIGSTOP keeps an export alive ----
            os.kill(p1.pid, signal.SIGSTOP)
            time.sleep(2.0)
            assert run("status", root)["exports"]["active"] == 2
            os.kill(p1.pid, signal.SIGCONT)
            evidence["sigstop_protected"] = True

            # ---- Release: frozen outputs exclude later batches ----
            (gate1 / "continue").touch()
            (gate2 / "continue").touch()
            assert p1.communicate(timeout=30)[1] == b"" or p1.returncode == 0
            assert p2.wait(timeout=30) == 0
            f1.close(); f2.close()
            got1 = [json.loads(line) for line in out1.read_text(encoding="utf-8").splitlines()]
            got2 = [json.loads(line) for line in out2.read_text(encoding="utf-8").splitlines()]
            assert got1 == got2 == frozen_one
            evidence["frozen_output"] = {"batches": [x["batch"] for x in got1],
                                         "later_batches_excluded": True}
            wait_clean()

            # ---- Broken pipe: reader disappears, worker reaps within 5s ----
            gate3 = area / "gate3"
            rpipe, wpipe = os.pipe()
            p3 = subprocess.Popen(command("export", root), cwd=str(work),
                                  env=dict(env, POINTWISE_PAUSE="export_streaming",
                                           POINTWISE_GATE=str(gate3)),
                                  stdout=wpipe, stderr=subprocess.PIPE)
            os.close(wpipe); os.close(rpipe)
            wait_gate(gate3)
            (gate3 / "continue").touch()
            assert p3.wait(timeout=20) == 0
            wait_clean()
            evidence["broken_pipe_reaped"] = True

            # ---- Termination at each gate: staging/streaming/releasing ----
            for event in ("export_staged", "export_streaming", "export_releasing"):
                gate_k = area / ("gate-kill-" + event)
                p_k, f_k = start_paused_export(gate_k, area / ("kill-%s.jsonl" % event), event)
                ready_k = wait_gate(gate_k)
                if event == "export_staged":
                    assert Path(ready_k["detail"]["path"]).exists()
                p_k.kill()
                p_k.wait(timeout=15)
                f_k.close()
                wait_clean()
            evidence["killed_at_gates_reaped"] = ["export_staged", "export_streaming",
                                                  "export_releasing"]

            # ---- Service restart reclaims leftovers and exports can resume ----
            gate5 = area / "gate5"
            p5, f5 = start_paused_export(gate5, area / "restart.jsonl")
            wait_gate(gate5)
            assert run("status", root)["exports"]["active"] == 1
            p5.kill()
            p5.wait(timeout=15)
            f5.close()
            assert list((root / "exports").glob("*"))
            worker.terminate()
            worker.wait(timeout=20)
            assert worker.returncode is not None
            restarted = subprocess.Popen(command("run", root), cwd=str(work), env=env,
                                         stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            children.append(restarted)
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline:
                if not list((root / "exports").glob("*")):
                    break
                time.sleep(0.2)
            assert not list((root / "exports").glob("*"))
            post_status = run("status", root)
            assert post_status["exports"] == {"active": 0, "bytes": 0,
                                              "budget": 64 * 1024 * 1024}
            re_export = subprocess.run(command("export", root), cwd=str(work), env=env,
                                       capture_output=True, text=True, timeout=30)
            assert re_export.returncode == 0
            re_rows = [json.loads(line) for line in re_export.stdout.splitlines()]
            assert [x["batch"] for x in re_rows] == [x["batch"] for x in run("read", root)]
            evidence["restart_reclamation"] = {"exports": post_status["exports"],
                                               "reaccepted_batches": len(re_rows)}

            # ---- Real user LaunchAgent go-live (macOS GUI session only) ----
            evidence["launchd_live"] = _launchd_live(package, dest, area / "live-install",
                                                     python, env, work)

            final_status = run("status", root)
            evidence["final_status_exports"] = final_status["exports"]
            if demo:
                print(json.dumps(evidence, ensure_ascii=False, sort_keys=True))
            else:
                print("INSTALLED_RELEASE_OK: business, two slow exports, WAL truncate, "
                      "capacity rejections, liveness, broken pipe, kill/restart reclamation")
        finally:
            for child in children:
                if child.poll() is None:
                    child.kill()
                child.communicate(timeout=30)


def _launchd_live(package, entry_dest, live_dest, python, env, work):
    """Bootstrap the real agent on macOS; record (never fake) when unavailable."""
    if sys.platform != "darwin":
        return {"executed": False,
                "reason": "no macOS GUI login session on platform %r; launchctl absent"
                          % sys.platform}
    if not Path("/bin/launchctl").exists():
        return {"executed": False, "reason": "launchctl not present"}
    uid = os.getuid()
    probe = subprocess.run(["/bin/launchctl", "print", "gui/%s" % uid],
                           capture_output=True, text=True)
    if probe.returncode != 0:
        return {"executed": False, "reason": "no gui/%d domain: %s"
                                              % (uid, probe.stderr.strip()[:200])}
    label = None
    agent = None
    try:
        # Reuse the already-installed entry point; install into a separate live root.
        install = subprocess.run(
            [python, str(entry_dest / "app.pyz"), "install", str(package), str(live_dest),
             "--python", python], cwd=str(work), env=env, capture_output=True, text=True)
        if install.returncode != 0:
            return {"executed": False, "reason": "bootstrap failed: " + install.stderr[:300]}
        meta = json.loads(install.stdout)
        label = meta["label"]
        agent = Path.home() / "Library" / "LaunchAgents" / (label + ".plist")
        printed = subprocess.run(["/bin/launchctl", "print", "gui/%s/%s" % (uid, label)],
                                 capture_output=True, text=True)
        state = printed.returncode == 0
        return {"executed": True, "label": label, "plist": str(agent),
                "launchctl_print": printed.stdout[:2000], "loaded": state}
    finally:
        if label is not None:
            subprocess.run(["/bin/launchctl", "bootout", "gui/%s/%s" % (uid, label)],
                           capture_output=True, text=True)
        if agent is not None:
            with contextlib.suppress(FileNotFoundError):
                agent.unlink()


if __name__ == "__main__":
    verify(demo=len(sys.argv) > 1 and sys.argv[1] == "demo")
