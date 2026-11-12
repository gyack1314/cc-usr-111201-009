# 离线容量资料

arrival/deadline为同一整数时间轴，deadline允许等于arrival，此类任务在同刻派发前放弃。service表对每类型给1到max_batch全部时长，为严格正整数，服务期间不引入随机抖动。worker编号0起连续。队首指所有等待任务中(arrival,id)最小的一条，选择其type后只在同类型等待队列中按序组批，最多max_batch；未满批要等该类型最早任务arrival+wait_window。未就绪时不让后面的其他类型越过，但进行中的worker完成不受此限制。多worker同刻依次派发，每次都重新选择全局队首。

在deadline前开批即保留运行，完成时刻>deadline为违约，等于deadline为按时完成。放弃也计入违约分子；比例分母是全体输入任务，不只完成者。排队区间为[arrival,start)或[arrival,abandon)，服务不计排队。观察窗为[lo,hi)，忙时和空闲时在此窗内积分，成本只针对该窗；模拟继续到全部任务结局，结果同时注明全量结局和窗口口径。所有配置使用同一个请求文件，输入顺序不改变以ID决胜的事件语义。

domain仅看请求身份、到达范围和服务表，不组批或模拟事件。事件顺序、输出字段和
成本口径见[容量分析器说明](capacity-analyzer.md)。
