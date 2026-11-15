# 采样分析结果服务：慢导出发布

## 已有工程和运行条件

Python 3.9+标准库、SQLite，macOS 13+普通用户GUI登录会话；提供可执行Python绝对路径，不捆绑解释器。没有第三方依赖、在线账号或外部系统服务，不使用Docker。安装和数据均在可写本机普通目录，路径可以含中文和空格，无符号链接。工作目录可以不同于源码，安装后开发源码副本会移走。首次安装时没有旧版服务运行，不涉及升级、数据迁址、远程存储或断电保证。

限定一个入账工作者和最多两个活动导出；提交可以与导出交错。发布的是现有分析流水线，不能重写业务核心。pipeline.py的validate、normalize、merge、summarize、encoded、digest行为不变；jobs既有列、检查点及载荷格式兼容。Engine持久编排、导出和运维代码可以调整或增加模块。manual.py保留离线核账能力；正式运行保证覆盖安装app.pyz入口，不要求阻挡绕过程序直接改库。

## 输入及固定业务期望

submit接收UTF-8 JSON文件，例如：
{"batch":"示例","segments":[[{"id":"x","rev":1,"sensor":"s1","ts":61,"value":120,"deleted":false}]]}

每批1～16段，每段0～2000行；id和sensor非空，rev为正整数，ts和value为非布尔整数，deleted为布尔。每个id取最高修订，允许改站、改时、撤销和复活；同修订异内容整批拒绝。未删除样本按(sensor,ts//60)汇总sum/count。相同batch及相同内容重投返回原接纳回执，异内容报BATCH_CONFLICT，不改原账。

samples/a.json完成结果：
[{"sensor":"s1","minute":-1,"sum":7,"count":1},{"sensor":"s2","minute":1,"sum":-30,"count":2}]
samples/b.json完成结果：
[{"sensor":"s1","minute":0,"sum":100,"count":1}]

read返回按batch排序的列表，每项为{"batch":ID,"rows":结果列表,"receipt":完成回执}。接纳回执为{"batch":ID,"sha256":规范输入摘要}；完成回执另含groups和samples，分别为分组数和未删除样本数。结果与完成回执一起提交，未完成批次不出现在read中。完成后重投仍返回原接纳回执，不重复入账。checks.py保留固定期望、修订冲突、重开、共享载荷、空结果及幂等断言，不得删改原断言。

## 公开入口和包

源码入口：python3 ops.py。安装入口：ABS DEST/app.pyz，ABS为可执行Python绝对路径。每个参数为独立argv元素，不依赖shell激活、当前目录或PYTHONPATH。

- build OUT：生成OUT/app.pyz和manifest.json，清单为{"sha256":"app.pyz的SHA-256"}，返回{"package":OUT绝对路径}。
- install PACKAGE DEST --python ABS [--no-load]：校验后安装，返回app、root、plist、label。root为DEST/data。
- submit ROOT INPUT：返回原接纳回执。
- run ROOT --once：执行一次Engine.step，返回{"progressed":布尔}；run ROOT为常驻循环。
- read ROOT：返回已完成结果。
- status ROOT：保留pending、completed、required、inventory；inventory为已发布业务载荷摘要到字节数的映射。
- export ROOT：stdout为UTF-8 JSON Lines，一行对应read中的一个完整批次对象，字段及原回执不变，按batch排序；没有已完成批次时输出空流。

除export外，成功命令输出一个JSON值。错误非零退出并在stderr说明原因；export的stdout不能夹带日志。同源码重复构建字节相同，坏包不得改变已安装程序和启动配置。新增生产模块须纳入ops.py的CORE，安装包脱离源码仍可运行。

默认install写入用户Library/LaunchAgents并bootstrap gui/<uid>，RunAtLoad及KeepAlive为true；日志在DEST。启动参数为[ABS,安装app.pyz绝对路径,"run",ROOT绝对路径]。--no-load只在DEST生成launch-agent.plist，不代表真实自启动。agentcheck.py核对配置，--live还调用launchctl print。

## 导出运行要求

export在首次确定生产库读快照时接纳；export_opened观察事件标识该边界。每次输出恰为该时刻已完成批次，包含完整rows及原receipt，不漏、不重、不混入后来完成的批次。导出可以在快照准备完毕前等待，但不能等消费者读完才解除生产库读事务。不要求跨进程续接导出；消费方关闭管道或导出进程被终止时，该次输出可以不完整。

ROOT/export-config.json为可选配置，内容为{"budget_bytes":67108864}；缺省为64MiB。预算为4096～268435456的整数，场景开始前写入，此轮不要求热修改配置。导出载荷的临时文件、就绪文件及其数据库边车文件均放在ROOT/exports内，按文件逻辑长度总和计预算，目录、空锁文件及ROOT其他位置的少量登记元数据不计。不得将导出载荷藏到其他目录或数据库，也不得把整份导出缓存到内存；允许逐批读取及固定大小缓冲。已有业务blobs不在本轮回收范围。

同时活动导出最多两个，快照构建也算活动。共享相同快照是允许的，不规定每个导出必须复制一份。任意时刻受管导出载荷总字节数不得超过预算。没有名额或无法在预算内准备完整快照时，非零退出、stderr含EXPORT_CAPACITY，stdout必须为空；不能先输出一部分再拒绝。容量拒绝不能损害已接纳导出或阻止入账。现有读入输入及既有业务载荷不纳入导出预算。

慢消费者包括输出背压和进程被暂停。活动导出所需文件不能被回收，不能仅因超时或PID数字判断其失活。正常结束、断管或进程终止后，常驻run应在5秒内释放该导出的名额及无其他活动导出依赖的快照文件；SIGSTOP仍算存活，不得收回。服务重启须同样回收遗留文件及部分准备文件，不能永久保留全部历史快照。只要求本机进程退出恢复，不要求机器掉电或文件系统损坏恢复。

status新增exports对象，字段为active、bytes、budget，分别为活动导出数、上述受管文件总字节数及配置预算。在文件不变化的观察点，bytes须与exportlab.py observe的snapshot_bytes相同。没有导出、回收完成后active和bytes均为0；正在活动但导出结果为空时允许无载荷文件。

两个导出都到达export_streaming且暂停消费后，有限新增批次仍须完成。停止新增写入并保持两个导出暂停，exportlab.py observe ROOT --checkpoint的checkpoint首项须为0，wal_bytes不大于65536；不能结束导出或终止消费者来满足。这个判据在快照准备结束后检查，不要求准备期间生产WAL恒为零。随后释放导出，输出须与各自接纳前冻结的期望完全一致。

## 非答案脚手架及可重跑实验

exporter.py是正确但朴素的基线：用BEGIN固定生产库快照，按游标逐批写stdout并flush，直到消费者读完才rollback。它没有快照文件、预算、活动登记或回收机制。exportcheck.py在真实内存SQLite上核对排序、完整批次、原回执、排除待办及空导出；不是WAL或磁盘预算通过证明。

Gate使用POINTWISE_PAUSE指定事件、POINTWISE_GATE指定观察目录，命中写ready.json并等待continue文件；每个实例仅暂停一次。已有业务事件不变。导出保留：
- export_opened：已确定接纳快照，detail含completed。
- export_streaming：首行尚未输出；目标交付在这里已准备好完整快照并释放生产库读事务，detail含completed。
- export_releasing：输出结束或发生异常，尚未完成资源释放。
目标交付另提供export_staged：首次持久写出非空快照载荷后、快照尚未就绪，detail含path。用于观察部分准备被终止后的回收，不规定登记布局或锁方案。空导出不要求命中export_staged。

exportlab.py只生成有限输入和观察，不实现导出生命周期：
- feed ROOT --app APP [--python ABS] [--start 0] [--batches 4] [--groups 256] [--interval 0.05] [--manual-run]：经安装APP逐批submit，等待该批完成，逐行输出接纳回执与批次ID。默认由常驻服务加工；--manual-run经APP执行run --once，不能和常驻工作者混用。
- freeze ROOT OUT --app APP [--python ABS]：经安装read冻结完整期望列表。
- observe ROOT [--checkpoint]：列出ROOT/exports各文件长度及总长、生产WAL长度；--checkpoint实际执行有1秒busy_timeout的TRUNCATE并输出SQLite原始三元组。
- check EXPECTED LINES：把导出文件逐行解析后与冻结期望完整比较，返回匹配数和实际结果。
- wait GATE [--seconds 20]、resume GATE：等待或释放Gate。

例如先用feed生成start=0的4批，每批256个分组，停止写入后freeze；启动两个export并各停在export_streaming。此后用feed从start=100生成8批，保持两个导出暂停，再observe --checkpoint。最后resume并将各导出输出与冻结期望比较。这是有限压力输入，非吞吐优化任务。期望文件及Gate目录放在ROOT外。实验进程用subprocess.Popen持有，SIGSTOP/SIGCONT或kill后用wait/communicate核实状态；不得通过实验文件决定生产资源归属。

## 交付及上线回归

releasecheck.py已有可复现构建、坏包拒绝、移除源码副本、工作者互斥、原样例核账与安装export回归，目前只安装--no-load配置。将其扩展为本轮安装产物验证，保留原断言。核对两慢导出、持续入账、生产WAL截断和释放后完整输出；分开验证活动名额耗尽和预算不足均在输出前拒绝，旧导出不受影响。另覆盖暂停存活保护、消费方断管、export_staged/export_streaming/export_releasing处终止及服务重开回收，回收后可以重新接纳导出。容量测试使用足够大于4096字节的结果；不要求特定内部实现或文件数量。

真实上线使用默认安装的用户LaunchAgent，移走开发源码副本并从其他工作目录操作；仅由自启动进程完成新增批次及导出遗留资源回收，导出通过安装APP发起。保留launchctl print、安装read/status及WAL/快照占用的实际输出。终止常驻进程后确认launchd拉起新进程并完成回收及后续入账，不能用手工run冒充。结束时bootout本次标签并删除本次用户plist。没有GUI登录会话时必须记为未执行，不得把配置生成写成上线通过。

交付简短的安装、容量拒绝识别、进程终止恢复和卸载说明，记录可重跑命令、实际输出及未通过项目。不做导出断点续传、版本切换、通用查询或额外调度系统。

.pointwise-runtime.json使用version=1、kind=cli，setup为准备命令argv数组列表，verify/demo分别为核验及演示argv。基线verify为["python3","exportcheck.py","normal"]，demo为["python3","exportcheck.py","demo"]。交付时verify改为["python3","releasecheck.py"]，demo改为["python3","releasecheck.py","demo"]；verify须实际核对上述安装与上线结果，demo展示安装后的导出内容、原回执、WAL和快照占用，不能只打印成功。
