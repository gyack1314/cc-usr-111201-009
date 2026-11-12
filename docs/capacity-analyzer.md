# 离线容量分析器

`analysis_data.simulation`是纯 Python、无外部服务依赖的离散事件分析器。它读取
`data-contract.md`描述的请求元数据和服务时长表，在同一份到达日志上比较多个
批处理配置。输入中的`configurations`只提供`workers`、`max_batch`和
`wait_window`；分析器不会引入模型程序、随机抖动或对真实推理服务的调用。

## 固定调度规则

1. 每个worker一次只运行一个同类型批次，批次开批后不可抢占。
2. 等待队列按`(arrival, id)`排序。全局队首是队列中`(arrival, id)`最小的任务。
3. 选择全局队首的类型，只从该类型的等待队列按序取任务；批次达到
   `max_batch`，或该类型最早任务的`arrival + wait_window`已到，就可以开批。
4. 若队首类型未达到开批条件，空闲worker保持空闲，不能越过队首去调度
   后面的其他类型。
5. 有多个空闲worker时按`worker_id`从小到大领取；每次领取后重新选择全局
   队首。
6. 服务时长取该类型、该**实际批大小**对应的表值，而不是`max_batch`对应的
   表值。

同一时刻的处理顺序固定为：

1. 完成到点的批次并释放worker；
2. 处理该时刻到达的全部任务；
3. 放弃`deadline <= 当前时刻`且尚未开批的任务；
4. 派发就绪批次。

因此，`deadline == arrival`的任务在到达同刻派发前放弃；在deadline之前开批
的任务即使完成时刻超过deadline也运行到底。完成时刻`> deadline`记完成但
违约，`== deadline`记按时完成。未开批任务到期只记放弃，不会同时记完成。

## 事件驱动与结局

模拟时间从`min(观察窗起点, 最早到达)`开始，只跳到下一个完成、到达、等待
到期或截止时刻，不逐毫秒忙等。若存在等待批但暂时没有进行中批次，等待到期
或截止时刻仍是可推进事件，不会提前结束。全部任务最终恰好有一个结局：
`completed`或`abandoned`。同一输入和配置重复运行会得到完全一致的任务、
批次与事件记录；重排输入请求顺序也不改变语义。

## 单配置结果

`simulate(value, config_id)`返回：

- `config`：本次使用的配置；
- `tasks`：按`(arrival, id)`排序的逐任务记录，包含`status`、`start`、
  `completion`、`abandon_time`、`batch_id`、`worker`、`wait_time`、
  `response_time`、`time_to_outcome`、`violation`；
- `batches`：每次开批的worker、类型、实际批大小、任务ID、服务时长与完成时刻；
- `events`：带递增`seq`的`enqueue`、`start`、`complete`、`abandon`记录；
- `metrics`：全量结局口径与观察窗口径的指标；
- `scope`：注明结局为全量模拟，排队/忙时/成本为观察窗。

`response_time`只对完成任务给出`completion - arrival`；放弃任务没有完成时刻，
其`time_to_outcome`为`abandon_time - arrival`。违约率分子是“完成但超过
deadline”加“放弃”，分母是全体输入任务，不只完成任务数。

观察窗`[lo, hi)`指标包括：

- `queue_area_window`：等待任务并发数在窗内的事件区间积分；
- `queue_area_window_crosscheck`：逐任务计算其排队区间`[arrival, start)`或
  `[arrival, abandon_time)`与窗口重叠时长后求和；
- `queue_area_crosscheck_matches`：两种算法是否一致；
- `worker_busy_time_window`、`worker_idle_time_window`及逐worker忙时；
- `cost_window`：`busy_time * busy_rate + idle_time * idle_rate`，按
  `cost_per_time`预先给定的费用表以`Fraction`精确计算。

排队面积不使用“完成者平均等待 × 人数”之类反推。窗口长度为零时，时间平均
排队长度和worker利用率返回`None`，避免伪装的数值。

## 多配置比较

`compare(value)`（`analyze`的别名）对全部配置运行同一份请求日志和服务表，
输出`configurations`以及便于并排查看的`comparison`行。`comparison`含完成数、
放弃数、违约率、平均等待、完成者平均响应、窗口排队面积、窗口忙时、利用率
和窗口成本。调用方可先用`load`取得`source_sha256`，再通过
`analyze(value, source_sha256=digest)`写入输出，证明比较使用的是同一输入。

观察窗只是这段有限日志覆盖的短窗，不能代表稳态容量；所有比较结论仅在同一
输入日志、同一服务表、同一费用表下成立。分析器不会声称某个批大小配置是
长期最优。

## 命令

```sh
python -m analysis_data.cli compare data/normal.json
python -m analysis_data.cli simulate data/normal.json --config single --config batch
python -m analysis_data.cli compare data/normal.json --output /tmp/result.json
```

`--output`拒绝与输入文件相同的路径。`inspect`只展示原始资料摘要，不运行
模拟。
