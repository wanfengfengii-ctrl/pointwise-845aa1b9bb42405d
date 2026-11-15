# 本轮初始非答案基线核验

读取来源为用户指定的568bd5a040ee4fec/source及其上级verification.json，仅检查原始非答案基线，没有读取最终答案。实际重新计算用户列出的13个文件摘要，全部相符；独立阅读业务核、持久化、发布、自启动检查、原回归及原迁址契约。

本轮保留pipeline.py、localio.py、manual.py、checks.py、agentcheck.py及两个样例原文。ops.py仅加入朴素export分支和打包模块；releasecheck.py仅加入安装export与已有read的完整对照。移除原题relocatelab.py，替换迁址契约。新增exporter.py、exportcheck.py和exportlab.py；没有快照隔离、预算登记、存活判定或回收实现。

实际环境为macOS受限只读工作区，Python 3.9.6。新文件以本次输出中的完整内容提供，未写入只读工作区。

实际执行：
1. 原路径argv为["env","PYTHONDONTWRITEBYTECODE=1","python3","checks.py","normal"]，退出0，stdout为：
NORMAL_REGRESSION_OK: revisions, tombstones, shared payloads, resume, receipts
2. 使用python3 -c将本次提供的Python源码在内存注册成模块，调用原checks.normal和新增exportcheck.normal；样例从原路径读取且内容保持不变。退出0，输出上述业务成功标记及：
EXPORT_BUSINESS_OK: ordered complete batches, original receipts, pending excluded, empty stream
这不是新文件已落盘执行的记录。
3. 同次内存检查compile全部10个Python文件；以只读内存文件适配器调用ops.bundle两次，断言包字节相同、ZIP校验通过、模块集合完整且归档源码全部可编译。输出：
IN_MEMORY_PACKAGE_OK fe634385598df89ff944655223828647c109be1fa5a8c9aab876bcc3019e25e7
ALL_PROVIDED_PYTHON_COMPILED 10
4. 原13个文件重新计算摘要输出：
ORIGINAL_SOURCE_HASHES_OK 13

解释器对xcrun临时缓存创建报告受限警告，上述运行均退出0。未尝试突破只读限制。

未执行：checks.py disk、写盘build/install、releasecheck.py磁盘回归、exportlab的真实并发WAL观察，以及真实LaunchAgent。没有把内存业务核验、归档检查或历史记录写成这些项目的通过证据。最终做题交付须在契约规定的可写GUI登录环境完成安装、慢读、预算、终止回收与真实自启动上线回归。

本基线正常业务和流式结果已经核对；目标运维保证尚未实现。复核依据来自本轮实际源码：exporter.stream把BEGIN至rollback的读事务包住stdout逐行write/flush；ops.run仅驱动业务step，localio.worker只提供工作者互斥。不能直接沿用这些机制解决快照预算与崩溃回收。

---

# 本轮慢导出交付核验（实际执行）

实际环境：Linux aarch64 容器，Python 3.11.2，SQLite 3.40.1；无 launchctl、无 ~/Library 与 gui/<uid> 域。真实用户 LaunchAgent 上线按契约记为“未执行”，未以 --no-load 配置冒充；releasecheck.py 的 launchd_live 字段如实输出 executed=false 及原因。

改动范围：新增 exportmgr.py（受管快照导出、磁盘预算、flock 存活与回收）；ops.py 仅集成新模块、status 增加 exports、常驻/单次 run 接入 reap 与 WAL TRUNCATE；releasecheck.py 保留原断言并扩展安装产物回归；新增 RECOVERY.md；.pointwise-runtime.json 的 verify/demo 改为 releasecheck。pipeline.py、localio.py、manual.py、checks.py、agentcheck.py、exporter.py、exportlab.py 与 samples 原样未改。

实际执行（均退出 0，工作区外临时目录、安装包脱离源码副本）：
1. python3 checks.py normal / disk：业务固定期望、修订冲突、重开、共享载荷、空结果、幂等及落盘恢复全部通过。
2. python3 exportcheck.py normal：朴素基线流式断言（排序、完整批次、原回执、排除待办、空导出、事件序列、事务结束）保持通过。
3. python3 releasecheck.py（verify）：可复现构建字节相同、坏包拒绝且不改变安装、工作者互斥、样例核账、安装导出对照；两个 export 停在 export_streaming 后常驻 worker 持续完成新增 4 批；exportlab observe --checkpoint 输出三元组首项 0、wal_bytes=0、snapshot_bytes 与 status.exports.bytes 相等；名额耗尽与 4096 字节预算不足均在 stdout 为空时以 EXPORT_CAPACITY 拒绝且旧导出不受影响；SIGSTOP 暂停期间 active 保持 2；恢复后两导出输出与冻结期望逐行相等、后续批次不混入；断管、export_staged/export_streaming/export_releasing 处 kill、服务重启遗留回收均在 5 秒内 active/bytes 归零，回收后可重新接纳。
4. python3 releasecheck.py demo：打印真实结果、原回执、预算拒绝 stderr、WAL/快照占用与 launchd 未执行原因，非仅成功标记。
5. 按 OPERATIONS.md 示例用 exportlab 复现：feed 4 批→freeze→双导出 export_streaming 暂停→再 feed 8 批→observe --checkpoint=[0,0,0]、wal_bytes=0→resume 后两输出均 matched_batches=4→5 秒内回收。
6. 边界：空 ROOT 导出空流；预算 4096/268435456 边界接受、越界与缺字段报 EXPORT_CONFIG；registry 损坏报 EXPORT_REGISTRY 且 stdout 为空；run --once 启动即回收上次遗留；共享快照在紧预算下不重复占字节；中文与空格安装路径全程可用。

未执行项：真实 launchctl bootstrap/print/bootout、kill 常驻进程后 launchd 拉起验证——本环境无 macOS GUI 登录会话，须在规定环境补跑；RECOVERY.md 第 4 节列出上线命令与判据。不包含断电/文件系统损坏恢复、断点续传、版本迁址或通用查询。
