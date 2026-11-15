# 慢导出交付：安装、容量拒绝、终止恢复与卸载说明

本文件随发布包源码一起提供，仅依赖 Python 3.9+ 标准库与 SQLite，无第三方依赖。
业务核（`pipeline.py` 的 validate/normalize/merge/summarize/encoded/digest）、
批次幂等、既有列/检查点/载荷格式与原回执均未改动；`manual.py` 离线核账保留。

## 1. 构建与安装（可校验、脱离源码）

每个参数都是独立 argv 元素，不依赖当前目录、PYTHONPATH 或 shell 激活。

```
python3 ops.py build OUT
# 产物：OUT/app.pyz、OUT/manifest.json，清单为 {"sha256": "app.pyz 的 SHA-256"}
python3 ops.py install OUT DEST --python "$(command -v python3)"
# 返回 app、root(=DEST/data)、plist、label
```

- 同源码重复构建字节相同；坏包（哈希不符、ZIP 损坏、模块缺失）在任何安装写入前被拒绝，
  已安装程序与启动配置不变。
- 默认安装写入 `~/Library/LaunchAgents/<label>.plist` 并 `launchctl bootstrap gui/<uid>`，
  RunAtLoad/KeepAlive 为 true，启动参数为 `[ABS, DEST/app.pyz, run, DEST/data]`，日志在 DEST。
- `--no-load` 只生成 `DEST/launch-agent.plist`，不代表真实自启动。
- 安装后开发源码副本可移走；所有命令经 `ABS DEST/app.pyz ...` 从其他工作目录执行。

## 2. 导出语义与容量拒绝识别

`export ROOT` 输出 UTF-8 JSON Lines，一行一个完整批次对象（`batch`/`rows`/`receipt`
字段与原回执不变，按 batch 排序）。接纳时刻固定快照，之后入账的批次不会混入；
没有已完成批次时输出空流（stdout 为空、退出 0）。

受管文件全部在 `ROOT/exports`：

| 文件 | 含义 | 是否计预算 |
| --- | --- | --- |
| `export-<id>.lock` | 每导出 flock 存活锁（空文件） | 否（空锁） |
| `.staging-<id>-<key>.jsonl` | 快照部分准备文件 | 是 |
| `snapshot-<key>.jsonl` | 就绪的内容寻址快照，可被两个导出共享 | 是 |

预算配置 `ROOT/export-config.json`（可选）：`{"budget_bytes": N}`，
N 为 4096～268435456 的整数，缺省 64MiB（67108864）。登记元数据
`ROOT/export-registry.json` 与注册表锁不含载荷、不计预算。

两类拒绝都在**任何 stdout 输出之前**发生，stdout 必为空，退出码非 0，stderr 含
`EXPORT_CAPACITY`：

1. 名额耗尽：活动导出已达两个（快照构建期间也算活动）。
   提示形如 `EXPORT_CAPACITY: no active export slot (limit 2)`。
2. 预算不足：准备完整快照会使受管文件逻辑长度总和超过预算。
   提示形如 `EXPORT_CAPACITY: snapshot exceeds budget 67108864 bytes`。
   容量拒绝不会删除或损坏已接纳导出的文件，也不阻止入账；拒绝后无残留载荷。

配置非法时 stderr 含 `EXPORT_CONFIG`（例如 budget_bytes 越界或不是整数）。

观察占用：

```
ABS DEST/app.pyz status ROOT
# exports: {"active": 活动导出数, "bytes": ROOT/exports 受管文件总字节, "budget": 配置预算}
python3 exportlab.py observe ROOT            # 逐文件长度、snapshot_bytes、生产 WAL 长度
python3 exportlab.py observe ROOT --checkpoint  # 1 秒 busy_timeout 的 TRUNCATE 原始三元组
```

在文件不变化的观察点，`status.exports.bytes` 与 `observe` 的 `snapshot_bytes` 相同；
没有导出或回收完成后 active、bytes 均为 0。

## 3. 慢消费、WAL 截断与进程终止恢复

- 快照在一个短的只读事务内冻结并落盘，事务在首行输出前结束（读连接关闭）。
  之后的输出背压、SIGSTOP 暂停都不持有生产库读事务；常驻 worker 在每个加工步与
  空转循环执行 `PRAGMA wal_checkpoint(TRUNCATE)`，慢读不会拖住生产 WAL 回收。
- 存活判定只认 flock：SIGSTOP 仍持锁，视为存活，文件绝不回收；不使用超时或 PID 猜测。
- 正常结束、消费方断管（EPIPE，导出安静退出 0）、进程被 kill 后，常驻 `run` 在
  5 秒内释放名额，并删除无其他活动导出引用的快照与部分准备文件。
- 服务重启（worker 被 launchd 拉起或手工重开）时第一步即执行回收，清理上次遗留的
  锁、暂存与孤儿快照；回收后可立即重新接纳导出。不保留全部历史快照，不支持断点续传。
- Gate 事件（`POINTWISE_PAUSE`/`POINTWISE_GATE`，每实例最多暂停一次）：
  `export_opened`（接纳边界，detail.completed）、`export_staged`（首次持久化非空快照后、
  未就绪，detail.path；空导出不命中）、`export_streaming`（首行输出前，快照已就绪且
  生产读事务已释放，detail.completed）、`export_releasing`（输出结束或异常、释放前）。

### 终止后人工核查步骤

```
ABS DEST/app.pyz status ROOT          # active 应在 5 秒内回到 0、bytes 回到 0
ls -la ROOT/exports                   # 回收完成后目录为空（目录本身保留）
python3 exportlab.py observe ROOT     # snapshot_files 为空、snapshot_bytes=0
ABS DEST/app.pyz export ROOT          # 可重新接纳，输出当前完整快照
```

若常驻服务未运行，启动一次 `ABS DEST/app.pyz run ROOT`（或等待 launchd 拉起），
进入循环即回收；`run ROOT --once` 也会在加工前先执行一次回收。

## 4. 上线回归（可重跑）

```
python3 releasecheck.py        # verify：构建/坏包/迁址/工作者互斥/样例核账/安装导出/
                               # 双慢导出+持续入账+WAL 截断/名额与预算拒绝/SIGSTOP 存活/
                               # 断管/三个 Gate 点终止/服务重启回收/重新接纳
python3 releasecheck.py demo   # 打印真实结果、原回执、WAL 与快照占用证据
python3 exportcheck.py normal  # 朴素基线业务断言（保留）
python3 checks.py normal       # 业务核固定期望、修订冲突、重开、共享载荷、幂等
python3 checks.py disk         # 落盘恢复
```

压力场景（与 OPERATIONS.md 示例一致，可用 exportlab 复现）：feed 4 批 → freeze →
两个 export 停在 export_streaming → 再 feed 8 批 → `observe --checkpoint`
要求三元组首项 0、wal_bytes ≤ 65536 → resume 后两个输出与冻结期望逐行相等 →
5 秒内 active/bytes 归零。

### 真实 LaunchAgent 上线

仅在 macOS 且存在 `gui/<uid>` 域时执行；流程：默认 install（不带 --no-load）→
`launchctl print gui/<uid>/<label>` 留存输出 → 经安装 APP 发起导出并由自启动 worker
完成新增批次与遗留回收 → kill 常驻进程确认 launchd 拉起新进程并完成回收与后续入账。
结束时 `launchctl bootout gui/<uid>/<label>` 并删除本次用户 plist。
没有 GUI 登录会话（如本次 Linux 容器、无 launchctl）时必须记为“未执行”，
`releasecheck.py` 的 `launchd_live.executed=false` 即如实标注，不得把 --no-load
配置生成当作上线通过。

## 5. 卸载

```
launchctl bootout gui/$(id -u)/<label>          # 若已 bootstrap
rm -f ~/Library/LaunchAgents/<label>.plist
rm -rf DEST                                     # 含 app.pyz、data、日志与 launch-agent.plist
```

删除 DEST 即同时移除生产库、业务 blobs 与 `exports` 受管文件。不涉及远程存储或
系统级服务；不需要额外调度。

## 6. 明确不做

导出断点续传、版本/数据迁址、通用查询接口、额外调度系统、机器掉电或文件系统损坏恢复。
