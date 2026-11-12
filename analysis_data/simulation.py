"""确定性的离线批处理排队模拟器。

本模块只使用请求元数据、服务时长表和配置，不调用任何真实推理服务，也不
使用机器真实时间或随机数。同一份输入与配置每次都会得到相同的事件轨迹。

事件处理顺序严格为：

1. 完成到点的批次并释放 worker；
2. 处理当前时刻到达的全部任务；
3. 放弃 ``deadline <= 当前时刻`` 且尚未开批的任务；
4. 按 worker_id 顺序派发就绪批次。

等待队列始终按 ``(arrival, id)`` 排序。队首任务所属类型未达到开批条件时，
空闲 worker 不会被派给后面的其他类型。
"""

from dataclasses import dataclass, field
from fractions import Fraction
from typing import Optional


# ---------------------------------------------------------------------------
# 输入校验
# ---------------------------------------------------------------------------

def _require_int(value, name):
    if type(value) is not int:
        raise ValueError(f"{name} 必须是整数")
    return value


def _require_nonempty_string(value, name):
    if not isinstance(value, str) or not value:
        raise ValueError(f"{name} 必须是非空字符串")
    return value


def _parse_service(service):
    if not isinstance(service, dict) or not service:
        raise ValueError("service 必须是非空对象")
    parsed = {}
    for task_type, table in service.items():
        _require_nonempty_string(task_type, "service 类型")
        if not isinstance(table, dict) or not table:
            raise ValueError(f"service[{task_type}] 必须是非空对象")
        parsed_table = {}
        for raw_size, raw_duration in table.items():
            if isinstance(raw_size, bool):
                raise ValueError("批大小必须为正整数")
            try:
                size = int(raw_size) if isinstance(raw_size, (str, int)) else None
            except ValueError:
                size = None
            if size is None or size <= 0 or str(size) != str(raw_size).strip():
                raise ValueError(f"service[{task_type}] 的批大小必须是正整数")
            duration = _require_int(raw_duration, f"service[{task_type}][{raw_size}]")
            if duration <= 0:
                raise ValueError("服务时长必须是严格正整数")
            parsed_table[size] = duration
        parsed[task_type] = parsed_table
    return parsed


def _parse_requests(requests, service):
    if not isinstance(requests, list):
        raise ValueError("requests 必须是数组")
    parsed = []
    seen_ids = set()
    for index, row in enumerate(requests):
        if not isinstance(row, dict):
            raise ValueError(f"requests[{index}] 必须是对象")
        task_id = _require_nonempty_string(row.get("id"), f"requests[{index}].id")
        if task_id in seen_ids:
            raise ValueError("任务身份必须非空且唯一")
        seen_ids.add(task_id)
        arrival = _require_int(row.get("arrival"), f"requests[{index}].arrival")
        deadline = _require_int(row.get("deadline"), f"requests[{index}].deadline")
        if arrival < 0:
            raise ValueError("arrival 不能为负")
        if deadline < arrival:
            raise ValueError("deadline 不能早于 arrival")
        task_type = _require_nonempty_string(row.get("type"), f"requests[{index}].type")
        if task_type not in service:
            raise ValueError(f"未知任务类型: {task_type}")
        parsed.append(
            {
                "id": task_id,
                "arrival": arrival,
                "deadline": deadline,
                "type": task_type,
            }
        )
    return parsed


def _parse_configurations(configurations, service, requests):
    if not isinstance(configurations, list) or not configurations:
        raise ValueError("configurations 必须是非空数组")
    parsed = []
    seen_ids = set()
    used_types = {row["type"] for row in requests}
    for index, row in enumerate(configurations):
        if not isinstance(row, dict):
            raise ValueError(f"configurations[{index}] 必须是对象")
        config_id = _require_nonempty_string(row.get("id"), f"configurations[{index}].id")
        if config_id in seen_ids:
            raise ValueError("配置身份必须唯一")
        seen_ids.add(config_id)
        workers = _require_int(row.get("workers"), f"configurations[{index}].workers")
        max_batch = _require_int(row.get("max_batch"), f"configurations[{index}].max_batch")
        wait_window = _require_int(row.get("wait_window"), f"configurations[{index}].wait_window")
        if workers < 1:
            raise ValueError("workers 必须至少为 1")
        if max_batch < 1:
            raise ValueError("max_batch 必须至少为 1")
        if wait_window < 0:
            raise ValueError("wait_window 不能为负")
        for task_type in sorted(used_types):
            missing = [size for size in range(1, max_batch + 1) if size not in service[task_type]]
            if missing:
                raise ValueError(
                    f"配置 {config_id} 缺少类型 {task_type} 的批大小服务时长: {missing}"
                )
        parsed.append(
            {
                "id": config_id,
                "workers": workers,
                "max_batch": max_batch,
                "wait_window": wait_window,
            }
        )
    return parsed


def _parse_window(window):
    if not isinstance(window, (list, tuple)) or len(window) != 2:
        raise ValueError("observation_window 必须是两个整数")
    lo = _require_int(window[0], "observation_window[0]")
    hi = _require_int(window[1], "observation_window[1]")
    if hi < lo:
        raise ValueError("observation_window 的结束不能早于开始")
    return (lo, hi)


def _parse_rate(value, name):
    if isinstance(value, bool):
        raise ValueError(f"{name} 必须是有限非负数")
    if isinstance(value, int):
        rate = Fraction(value)
    elif isinstance(value, float):
        if value != value or value in (float("inf"), float("-inf")):
            raise ValueError(f"{name} 必须是有限非负数")
        rate = Fraction(str(value))
    elif isinstance(value, str):
        try:
            rate = Fraction(value)
        except (ValueError, ZeroDivisionError):
            raise ValueError(f"{name} 必须是分数或有限数值")
    else:
        raise ValueError(f"{name} 必须是分数或有限数值")
    if rate < 0:
        raise ValueError(f"{name} 不能为负")
    return rate


def _parse_costs(value):
    if not isinstance(value, dict):
        raise ValueError("cost_per_time 必须是对象")
    return {
        "busy": _parse_rate(value.get("busy"), "cost_per_time.busy"),
        "idle": _parse_rate(value.get("idle"), "cost_per_time.idle"),
    }


def validate_input(value):
    """校验并规范化一份离线容量资料。返回的对象不修改输入。"""
    if not isinstance(value, dict):
        raise ValueError("资料顶层必须是对象")
    service = _parse_service(value.get("service"))
    requests = _parse_requests(value.get("requests"), service)
    configurations = _parse_configurations(value.get("configurations"), service, requests)
    window = _parse_window(value.get("observation_window"))
    costs = _parse_costs(value.get("cost_per_time"))
    return {
        "requests": requests,
        "service": service,
        "configurations": configurations,
        "observation_window": window,
        "cost_per_time": costs,
    }


# ---------------------------------------------------------------------------
# 模拟核心
# ---------------------------------------------------------------------------

@dataclass
class _Task:
    id: str
    arrival: int
    deadline: int
    type: str
    status: str = "unreleased"
    start: Optional[int] = None
    completion: Optional[int] = None
    abandon_time: Optional[int] = None
    batch_id: Optional[str] = None
    worker: Optional[int] = None


@dataclass
class _Batch:
    id: str
    worker: int
    type: str
    tasks: list
    size: int
    start: int
    service: int
    completion: int


@dataclass
class _Worker:
    id: int
    batch: Optional[_Batch] = None


@dataclass
class _Trace:
    events: list = field(default_factory=list)

    def add(self, time, kind, **fields):
        event = {"seq": len(self.events), "time": time, "kind": kind}
        event.update(fields)
        self.events.append(event)


def _fraction_text(value):
    if value.denominator == 1:
        return str(value.numerator)
    return f"{value.numerator}/{value.denominator}"


class _Simulator:
    def __init__(self, requests, service, config, window):
        self.requests = sorted(requests, key=lambda row: (row["arrival"], row["id"]))
        self.service = service
        self.config = config
        self.window = window
        self.tasks = [
            _Task(row["id"], row["arrival"], row["deadline"], row["type"])
            for row in self.requests
        ]
        self.workers = [_Worker(worker_id) for worker_id in range(config["workers"])]
        self.waiting = {task_type: [] for task_type in service}
        self.arrival_index = 0
        self.batches = []
        self.trace = _Trace()
        self.clock = self._initial_clock()
        self._next_batch_number = 0

    def _initial_clock(self):
        first_arrival = self.requests[0]["arrival"] if self.requests else None
        if first_arrival is None:
            return self.window[0]
        return min(self.window[0], first_arrival)

    # -- 队列与派发 --------------------------------------------------------

    def _enqueue(self, task):
        task.status = "waiting"
        self.waiting[task.type].append(task)
        # 到达按 (arrival, id) 排序处理，因此同类列表天然保持有序。
        self.trace.add(task.arrival, "enqueue", task=task.id, type=task.type)

    def _head(self):
        candidates = [queue[0] for queue in self.waiting.values() if queue]
        if not candidates:
            return None
        return min(candidates, key=lambda task: (task.arrival, task.id))

    def _process_completions(self):
        for worker in self.workers:
            batch = worker.batch
            if batch is not None and batch.completion == self.clock:
                for task in batch.tasks:
                    task.status = "completed"
                    task.completion = self.clock
                self.trace.add(
                    self.clock,
                    "complete",
                    batch=batch.id,
                    worker=worker.id,
                    type=batch.type,
                    task_ids=[task.id for task in batch.tasks],
                    size=batch.size,
                )
                worker.batch = None

    def _process_arrivals(self):
        while (
            self.arrival_index < len(self.requests)
            and self.requests[self.arrival_index]["arrival"] == self.clock
        ):
            task = self.tasks[self.arrival_index]
            self._enqueue(task)
            self.arrival_index += 1

    def _process_expiries(self):
        expired = []
        for queue in self.waiting.values():
            keep = []
            for task in queue:
                if task.deadline <= self.clock:
                    expired.append(task)
                else:
                    keep.append(task)
            queue[:] = keep
        for task in sorted(expired, key=lambda item: (item.arrival, item.id)):
            task.status = "abandoned"
            task.abandon_time = self.clock
            self.trace.add(
                self.clock,
                "abandon",
                task=task.id,
                type=task.type,
                deadline=task.deadline,
            )

    def _new_batch_id(self):
        batch_id = f"b{self._next_batch_number:04d}"
        self._next_batch_number += 1
        return batch_id

    def _next_idle_worker(self):
        for worker in self.workers:
            if worker.batch is None:
                return worker
        return None

    def _dispatch(self):
        while True:
            worker = self._next_idle_worker()
            if worker is None:
                return
            head = self._head()
            if head is None:
                return
            queue = self.waiting[head.type]
            size = min(self.config["max_batch"], len(queue))
            ready = size == self.config["max_batch"] or (
                self.clock >= queue[0].arrival + self.config["wait_window"]
            )
            if not ready:
                # 队首类型未就绪时不越过它调度其他类型。
                return

            batch_id = self._new_batch_id()
            selected = queue[:size]
            del queue[:size]
            duration = self.service[head.type][size]
            batch = _Batch(
                id=batch_id,
                worker=worker.id,
                type=head.type,
                tasks=selected,
                size=size,
                start=self.clock,
                service=duration,
                completion=self.clock + duration,
            )
            worker.batch = batch
            self.batches.append(batch)
            for task in selected:
                task.status = "running"
                task.start = self.clock
                task.batch_id = batch_id
                task.worker = worker.id
            self.trace.add(
                self.clock,
                "start",
                batch=batch.id,
                worker=worker.id,
                type=batch.type,
                task_ids=[task.id for task in selected],
                size=batch.size,
                service=batch.service,
                completion=batch.completion,
            )

    # -- 事件推进 ----------------------------------------------------------

    def _next_event_time(self):
        candidates = []
        if self.arrival_index < len(self.requests):
            candidates.append(self.requests[self.arrival_index]["arrival"])
        for worker in self.workers:
            if worker.batch is not None:
                candidates.append(worker.batch.completion)
        for queue in self.waiting.values():
            for task in queue:
                candidates.append(task.deadline)

        head = self._head()
        idle_worker = self._next_idle_worker()
        if head is not None and idle_worker is not None:
            queue = self.waiting[head.type]
            size = min(self.config["max_batch"], len(queue))
            if size < self.config["max_batch"]:
                ready_at = queue[0].arrival + self.config["wait_window"]
                if ready_at > self.clock:
                    candidates.append(ready_at)

        future = [time for time in candidates if time > self.clock]
        if not future:
            return None
        return min(future)

    def _tick(self):
        self._process_completions()
        self._process_arrivals()
        self._process_expiries()
        self._dispatch()

    def run(self):
        while True:
            self._tick()
            unfinished = any(
                task.status in ("unreleased", "waiting", "running") for task in self.tasks
            )
            if not unfinished:
                return
            next_time = self._next_event_time()
            if next_time is None:
                # 仍在等待却没有任何可推进事件说明状态不自洽。
                raise RuntimeError("存在未完成工作但无法推进事件")
            self.clock = next_time


def _task_record(task):
    if task.status == "completed":
        if task.start is None or task.completion is None:
            raise RuntimeError("完成任务缺少开始或完成时刻")
        wait_time = task.start - task.arrival
        response_time = task.completion - task.arrival
        outcome_time = response_time
        violation = task.completion > task.deadline
    elif task.status == "abandoned":
        if task.abandon_time is None:
            raise RuntimeError("放弃任务缺少放弃时刻")
        wait_time = task.abandon_time - task.arrival
        response_time = None
        outcome_time = wait_time
        violation = True
    else:
        raise RuntimeError("模拟结束时仍有任务没有结局")
    return {
        "id": task.id,
        "type": task.type,
        "arrival": task.arrival,
        "deadline": task.deadline,
        "status": task.status,
        "start": task.start,
        "completion": task.completion,
        "abandon_time": task.abandon_time,
        "batch_id": task.batch_id,
        "worker": task.worker,
        "wait_time": wait_time,
        "response_time": response_time,
        "time_to_outcome": outcome_time,
        "violation": violation,
    }


def _batch_record(batch):
    return {
        "id": batch.id,
        "worker": batch.worker,
        "type": batch.type,
        "size": batch.size,
        "task_ids": [task.id for task in batch.tasks],
        "start": batch.start,
        "service": batch.service,
        "completion": batch.completion,
    }


def run_configuration(requests, service, config, window, costs):
    """按一组确定规则运行一个配置，返回任务记录、批记录、事件与指标。"""
    simulator = _Simulator(requests, service, config, window)
    simulator.run()
    tasks = [_task_record(task) for task in simulator.tasks]
    tasks.sort(key=lambda row: (row["arrival"], row["id"]))
    batches = [_batch_record(batch) for batch in simulator.batches]
    events = simulator.trace.events
    metrics = _metrics(tasks, batches, config["workers"], window, costs)
    return {
        "config": dict(config),
        "tasks": tasks,
        "batches": batches,
        "events": events,
        "metrics": metrics,
        "scope": {
            "outcomes": "full_simulation",
            "queue_and_worker_metrics": "observation_window",
        },
    }


# ---------------------------------------------------------------------------
# 指标：观察窗事件区间积分与逐任务交叉核对
# ---------------------------------------------------------------------------

def _overlap(interval_start, interval_end, lo, hi):
    return max(0, min(interval_end, hi) - max(interval_start, lo))


def _event_interval_area(intervals, lo, hi):
    """把若干 [start, end) 区间的并发数按事件区间积分，并裁剪到观察窗。"""
    deltas = {}
    for start, end in intervals:
        if start == end:
            continue
        deltas[start] = deltas.get(start, 0) + 1
        deltas[end] = deltas.get(end, 0) - 1
    times = sorted(deltas)
    area = 0
    count = 0
    previous = None
    for time in times:
        if previous is not None and time > previous:
            area += count * _overlap(previous, time, lo, hi)
        count += deltas[time]
        previous = time
    return area


def _mean(total, count):
    return None if count == 0 else total / count


def _metrics(tasks, batches, worker_count, window, costs):
    lo, hi = window
    length = hi - lo

    wait_intervals = []
    response_total_completed = 0
    completed_count = 0
    completed_violated_count = 0
    abandoned_count = 0
    violated_count = 0
    on_time_count = 0
    wait_total_all = 0
    wait_total_completed = 0
    wait_total_abandoned = 0
    outcome_total_all = 0

    for task in tasks:
        if task["status"] == "completed":
            end = task["start"]
            completed_count += 1
            response_total_completed += task["response_time"]
            wait_total_completed += task["wait_time"]
            if task["violation"]:
                completed_violated_count += 1
            else:
                on_time_count += 1
        else:
            end = task["abandon_time"]
            abandoned_count += 1
            wait_total_abandoned += task["wait_time"]
        wait_intervals.append((task["arrival"], end))
        wait_total_all += task["wait_time"]
        outcome_total_all += task["time_to_outcome"]
        if task["violation"]:
            violated_count += 1

    total = len(tasks)
    queue_area_events = _event_interval_area(wait_intervals, lo, hi)
    queue_area_tasks = sum(
        _overlap(start, end, lo, hi) for start, end in wait_intervals
    )
    if queue_area_events != queue_area_tasks:
        raise RuntimeError("排队面积事件积分与逐任务核对不一致")

    busy_by_worker = {worker_id: 0 for worker_id in range(worker_count)}
    batch_intervals = []
    for batch in batches:
        interval = (batch["start"], batch["completion"])
        batch_intervals.append(interval)
        busy_by_worker[batch["worker"]] += _overlap(interval[0], interval[1], lo, hi)
    busy_total = sum(busy_by_worker.values())
    busy_events = _event_interval_area(batch_intervals, lo, hi)
    if busy_total != busy_events:
        raise RuntimeError("worker 忙时逐批核对与事件积分不一致")
    idle_total = worker_count * length - busy_total
    if idle_total < 0:
        raise RuntimeError("观察窗内空闲时间不能为负")
    utilization = None if worker_count * length == 0 else busy_total / (worker_count * length)

    violation_rate = Fraction(violated_count, total) if total else Fraction(0, 1)
    cost = costs["busy"] * busy_total + costs["idle"] * idle_total

    return {
        "request_count": total,
        "completed_count": completed_count,
        "completed_violated_count": completed_violated_count,
        "abandoned_count": abandoned_count,
        "violated_count": violated_count,
        "on_time_count": on_time_count,
        "violation_rate": float(violation_rate),
        "violation_rate_exact": _fraction_text(violation_rate),
        "wait_time_total": wait_total_all,
        "wait_time_mean": _mean(wait_total_all, total),
        "wait_time_mean_completed": _mean(wait_total_completed, completed_count),
        "wait_time_mean_abandoned": _mean(wait_total_abandoned, abandoned_count),
        "response_time_total_completed": response_total_completed,
        "response_time_mean_completed": _mean(response_total_completed, completed_count),
        "time_to_outcome_total": outcome_total_all,
        "time_to_outcome_mean": _mean(outcome_total_all, total),
        "observation_window": [lo, hi],
        "window_length": length,
        "queue_area_window": queue_area_events,
        "queue_area_window_crosscheck": queue_area_tasks,
        "queue_area_crosscheck_matches": queue_area_events == queue_area_tasks,
        "queue_length_time_average_window": _mean(queue_area_events, length),
        "worker_busy_time_window": busy_total,
        "worker_idle_time_window": idle_total,
        "worker_busy_time_window_crosscheck": busy_events,
        "worker_busy_crosscheck_matches": busy_total == busy_events,
        "worker_utilization_window": utilization,
        "worker_busy_time_window_by_worker": [
            {
                "worker_id": worker_id,
                "busy_time": busy_by_worker[worker_id],
                "idle_time": length - busy_by_worker[worker_id],
            }
            for worker_id in range(worker_count)
        ],
        "cost_window": _fraction_text(cost),
        "cost_breakdown": {
            "busy_time": busy_total,
            "idle_time": idle_total,
            "busy_rate": _fraction_text(costs["busy"]),
            "idle_rate": _fraction_text(costs["idle"]),
        },
    }


# ---------------------------------------------------------------------------
# 多配置分析
# ---------------------------------------------------------------------------

def _input_summary(material):
    requests = material["requests"]
    counts = {}
    for row in requests:
        counts[row["type"]] = counts.get(row["type"], 0) + 1
    return {
        "request_count": len(requests),
        "types": dict(sorted(counts.items())),
        "first_arrival": min((row["arrival"] for row in requests), default=None),
        "last_arrival": max((row["arrival"] for row in requests), default=None),
        "observation_window": list(material["observation_window"]),
        "cost_per_time": {
            "busy": _fraction_text(material["cost_per_time"]["busy"]),
            "idle": _fraction_text(material["cost_per_time"]["idle"]),
        },
    }


def _comparison_row(result):
    config = result["config"]
    metrics = result["metrics"]
    return {
        "id": config["id"],
        "workers": config["workers"],
        "max_batch": config["max_batch"],
        "wait_window": config["wait_window"],
        "completed_count": metrics["completed_count"],
        "completed_violated_count": metrics["completed_violated_count"],
        "abandoned_count": metrics["abandoned_count"],
        "violation_rate": metrics["violation_rate"],
        "wait_time_mean": metrics["wait_time_mean"],
        "wait_time_mean_completed": metrics["wait_time_mean_completed"],
        "response_time_mean_completed": metrics["response_time_mean_completed"],
        "queue_area_window": metrics["queue_area_window"],
        "worker_busy_time_window": metrics["worker_busy_time_window"],
        "worker_utilization_window": metrics["worker_utilization_window"],
        "cost_window": metrics["cost_window"],
    }


def analyze(value, config_ids=None, source_sha256=None):
    """运行一份资料中的全部（或指定）配置，并给出可比较的汇总。

    所有配置读取同一份请求日志和服务表，只改变 workers、max_batch 与
    wait_window。观察窗内的成本与忙时只反映有限日志覆盖的时段，不代表
    稳态容量。
    """
    material = validate_input(value)
    configurations = material["configurations"]
    if config_ids is not None:
        requested = list(config_ids)
        by_id = {config["id"]: config for config in configurations}
        unknown = [config_id for config_id in requested if config_id not in by_id]
        if unknown:
            raise ValueError(f"未知配置: {unknown}")
        if not requested:
            raise ValueError("至少选择一个配置")
        configurations = [by_id[config_id] for config_id in requested]

    results = []
    for config in configurations:
        results.append(
            run_configuration(
                material["requests"],
                material["service"],
                config,
                material["observation_window"],
                material["cost_per_time"],
            )
        )

    output = {
        "input": _input_summary(material),
        "configurations": results,
        "comparison": [_comparison_row(result) for result in results],
        "caveat": (
            "所有配置使用同一请求日志；观察窗内忙时、排队面积和成本只是该有限"
            "日志口径，不能据此声称配置达到稳态容量或长期最优。"
        ),
    }
    if source_sha256 is not None:
        output["source_sha256"] = source_sha256
    return output


def simulate(value, config_id=None, source_sha256=None):
    """便捷入口。

    指定 ``config_id`` 时返回该配置的单项结果；否则返回 :func:`analyze`
    的多配置比较结果。
    """
    if config_id is None:
        return analyze(value, source_sha256=source_sha256)
    material = validate_input(value)
    matches = [config for config in material["configurations"] if config["id"] == config_id]
    if not matches:
        raise ValueError(f"未知配置: {config_id}")
    result = run_configuration(
        material["requests"],
        material["service"],
        matches[0],
        material["observation_window"],
        material["cost_per_time"],
    )
    if source_sha256 is not None:
        result["source_sha256"] = source_sha256
    return result


compare = analyze
