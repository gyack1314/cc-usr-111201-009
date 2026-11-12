# 端侧批处理容量的离散事件反事实分析

这是现场机器人应用分析的合成资料项目，不接入真实推理服务。`data`保存业务记录，`analysis_data.io`负责严格 JSON 读取和原始字节指纹，`analysis_data.domain`提供已有的基础汇总，`analysis_data.simulation`提供确定性的离线批处理容量分析。字段与数学口径见[资料约定](docs/data-contract.md)，事件规则和输出字段见[容量分析器说明](docs/capacity-analyzer.md)。所有厂区、设备、收益与结果均为合成，不代表任何实际项目的效果；附件只有请求元数据与服务时长表，没有模型程序。

Python 3.11及以上版本可直接运行，当前无第三方Python依赖。项目根目录中的常用命令：

```sh
python -m unittest discover -s tests -v
python -m compileall -q analysis_data tests
python -m analysis_data.cli inspect data/normal.json
python -m analysis_data.cli compare data/normal.json
python -m analysis_data.cli simulate data/normal.json --config batch
python -m analysis_data.cli compare data/normal.json --output /tmp/capacity.json
```

`compare`在同一份请求日志和服务表上运行全部配置，只改变`workers`、`max_batch`和`wait_window`。每次输出都带输入文件的`source_sha256`；`--output`拒绝覆盖输入文件。原始资料不被修改。

分析器按固定规则做事件驱动模拟：同刻先完成批次并释放worker，再处理全部到达，随后放弃`deadline <= 当前时刻`且尚未开批的任务，最后按worker_id顺序派发就绪批。等待队列按`(arrival, id)`排序；队首类型未达到`max_batch`且最早任务尚未等满`wait_window`时，空闲worker不会越过它去调度其他类型。已开批任务运行到底，完成时刻严格大于deadline记完成但违约；未开批到期记放弃，二者互斥。模拟跳到下一完成、到达、等待到期或截止时刻，不使用真实机器时间。

输出包含逐任务的排队/开始/完成/放弃记录，批次组批与服务时长，以及等待、总响应、违约率和worker忙时。观察窗`[lo,hi)`内的排队面积同时给出事件区间积分和逐任务重叠时长两种算法并核对一致；忙时、空闲时和成本只按该窗计算。成本使用资料中预先给定的忙时/空闲时费用表，以分数精确计算。观察窗是有限日志口径，不能据此声称任何配置达到稳态容量或长期最优。

`data`中的`normal.json`、`empty.json`、`waiting-tail.json`展示常规与边界输入；`synthetic-same-instant.json`、`synthetic-wait-tail.json`、`synthetic-contention.json`、`synthetic-blocking.json`分别固定同刻完成/到达/到期、窗口等待与尾批、多worker同刻抢队、队首阻塞等语义，测试对同一输入重复运行和重排请求顺序都得到相同轨迹。`invalid`前缀文件保存不合法输入。

容器可执行 `docker build -t field-analysis .` 后用 `docker run --rm field-analysis` 查看资料，或 `docker run --rm field-analysis python -m unittest discover -s tests -v` 运行现有检查。运行期间无外网或账号要求。
