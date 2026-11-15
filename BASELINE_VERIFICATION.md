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
