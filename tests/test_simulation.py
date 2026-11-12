"""核对确定性事件顺序、组批规则、结局与窗口口径。"""
import json
import unittest
from fractions import Fraction

from analysis_data.io import load
from analysis_data.simulation import analyze, simulate, validate_input


def _value(path):
    return load(path)[0]


def _run(path, config_id=None):
    return simulate(_value(path), config_id=config_id)


def _event_key(event):
    return (event["time"], event["kind"], event.get("task") or event.get("batch"))


class SameInstantTests(unittest.TestCase):
    def test_completion_arrival_and_expiry_are_ordered(self):
        result = _run("data/synthetic-same-instant.json", "one-worker")
        events = [_event_key(event) for event in result["events"]]
        self.assertEqual(
            events,
            [
                (0, "enqueue", "r00"),
                (0, "enqueue", "r01"),
                (0, "abandon", "r01"),
                (0, "start", "b0000"),
                (4, "complete", "b0000"),
                (4, "enqueue", "r02"),
                (4, "start", "b0001"),
                (6, "complete", "b0001"),
                (6, "enqueue", "r03"),
                (6, "abandon", "r03"),
            ],
        )

    def test_deadline_equal_completion_is_on_time_but_equal_arrival_abandons(self):
        result = _run("data/synthetic-same-instant.json", "one-worker")
        tasks = {task["id"]: task for task in result["tasks"]}
        # 在 deadline 前开批，完成时刻恰好等于 deadline：按时完成。
        self.assertEqual(tasks["r02"]["status"], "completed")
        self.assertEqual(tasks["r02"]["completion"], tasks["r02"]["deadline"])
        self.assertFalse(tasks["r02"]["violation"])
        # deadline 等于到达时刻：同刻派发前放弃，不会开始。
        self.assertEqual(tasks["r03"]["status"], "abandoned")
        self.assertIsNone(tasks["r03"]["start"])
        self.assertEqual(tasks["r03"]["abandon_time"], tasks["r03"]["deadline"])


class WindowAndTailTests(unittest.TestCase):
    def test_partial_batches_wait_for_window_and_tail_batch(self):
        result = _run("data/synthetic-wait-tail.json", "tail-window")
        batches = result["batches"]
        self.assertEqual(
            [(batch["id"], batch["size"], batch["start"], batch["service"], batch["completion"]) for batch in batches],
            [("b0000", 3, 2, 6, 8), ("b0001", 2, 8, 3, 11)],
        )
        tasks = {task["id"]: task for task in result["tasks"]}
        # 最早的同类型任务 a0 到达 0、等待窗 2，整批在 2 开批。
        self.assertEqual(tasks["a0"]["start"], 2)
        # b0/b1 在唯一 worker 忙完后以不足 max_batch 的尾批开批，服务取大小 2 的表。
        self.assertEqual(tasks["b0"]["start"], 8)
        self.assertEqual(tasks["b1"]["start"], 8)
        self.assertEqual(result["config"]["wait_window"], 2)

    def test_waiter_without_running_work_is_not_terminated_early(self):
        result = _run("data/synthetic-wait-tail.json", "tail-window")
        self.assertTrue(any(event["kind"] == "enqueue" for event in result["events"]))
        # 0..1 只有等待任务、没有进行中批次，模拟仍推进到 2 开批。
        self.assertEqual(min(batch["start"] for batch in result["batches"]), 2)

    def test_queue_area_matches_per_task_overlap(self):
        result = _run("data/synthetic-wait-tail.json", "tail-window")
        metrics = result["metrics"]
        self.assertTrue(metrics["queue_area_crosscheck_matches"])
        self.assertEqual(metrics["queue_area_window"], 18)
        self.assertEqual(metrics["queue_area_window_crosscheck"], 18)
        # 手工核对：a0,a1 各 2；tail 0；b0,b1 各 7。
        self.assertEqual(metrics["queue_area_window"], 2 + 2 + 0 + 7 + 7)

    def test_expiry_wins_over_batch_readiness_at_same_instant(self):
        value = {
            "requests": [{"id": "r", "arrival": 0, "deadline": 2, "type": "image"}],
            "service": {"image": {"1": 1, "2": 1, "3": 1}},
            "configurations": [{"id": "c", "workers": 1, "max_batch": 3, "wait_window": 2}],
            "observation_window": [0, 5],
            "cost_per_time": {"busy": "1", "idle": "0"},
        }
        task = simulate(value, "c")["tasks"][0]
        # 窗口到期与 deadline 同为 2；同刻先放弃，因此不会在 2 开批。
        self.assertEqual(task["status"], "abandoned")
        self.assertEqual(task["abandon_time"], 2)
        self.assertIsNone(task["start"])

    def test_deadline_abandons_while_worker_is_busy(self):
        value = {
            "requests": [
                {"id": "busy", "arrival": 0, "deadline": 10, "type": "image"},
                {"id": "late", "arrival": 0, "deadline": 2, "type": "text"},
            ],
            "service": {"image": {"1": 4}, "text": {"1": 2}},
            "configurations": [{"id": "c", "workers": 1, "max_batch": 1, "wait_window": 0}],
            "observation_window": [0, 5],
            "cost_per_time": {"busy": "1", "idle": "0"},
        }
        result = simulate(value, "c")
        tasks = {task["id"]: task for task in result["tasks"]}
        self.assertEqual(tasks["late"]["status"], "abandoned")
        self.assertEqual(tasks["late"]["abandon_time"], 2)
        self.assertEqual(tasks["busy"]["completion"], 4)


class BlockingTests(unittest.TestCase):
    def test_full_later_type_does_not_overtake_unready_head(self):
        result = _run("data/synthetic-blocking.json", "blocked-head")
        batches = result["batches"]
        self.assertEqual(
            [(batch["worker"], batch["type"], batch["size"], batch["start"]) for batch in batches],
            [(0, "image", 2, 2), (1, "text", 3, 2)],
        )
        # t=1 时 text 已满批，但 image 队首未到期，两个 worker 都不得越过它。
        self.assertFalse(any(event["kind"] == "start" and event["time"] == 1 for event in result["events"]))
        tasks = {task["id"]: task for task in result["tasks"]}
        self.assertEqual(tasks["b0"]["start"], 2)


class MultiWorkerTests(unittest.TestCase):
    def test_idle_workers_claim_in_worker_id_order(self):
        result = _run("data/synthetic-contention.json", "two-workers")
        starts_at_zero = [
            event for event in result["events"] if event["kind"] == "start" and event["time"] == 0
        ]
        self.assertEqual([event["worker"] for event in starts_at_zero], [0, 1])
        # 每次派发后都重新选择全局 (arrival,id) 队首。
        self.assertEqual([event["type"] for event in starts_at_zero], ["image", "text"])
        self.assertEqual(
            [(batch["id"], batch["worker"], batch["task_ids"]) for batch in result["batches"]],
            [
                ("b0000", 0, ["c0"]),
                ("b0001", 1, ["c1"]),
                ("b0002", 1, ["c2"]),
                ("b0003", 0, ["c3"]),
            ],
        )

    def test_service_comes_from_actual_batch_size(self):
        result = _run("data/synthetic-contention.json", "two-full-batches")
        batches = {batch["type"]: batch for batch in result["batches"]}
        service = _value("data/synthetic-contention.json")["service"]
        self.assertEqual(batches["image"]["size"], 2)
        self.assertEqual(batches["text"]["size"], 2)
        self.assertEqual(batches["image"]["service"], service["image"]["2"])
        self.assertEqual(batches["text"]["service"], service["text"]["2"])


class OutcomeTests(unittest.TestCase):
    def test_every_task_has_exactly_one_outcome(self):
        for path in [
            "data/normal.json",
            "data/empty.json",
            "data/synthetic-same-instant.json",
            "data/synthetic-wait-tail.json",
            "data/synthetic-contention.json",
            "data/synthetic-blocking.json",
        ]:
            value = _value(path)
            for result in analyze(value)["configurations"]:
                with self.subTest(path=path, config=result["config"]["id"]):
                    statuses = [task["status"] for task in result["tasks"]]
                    self.assertTrue(all(status in ("completed", "abandoned") for status in statuses))
                    self.assertEqual(len(statuses), result["metrics"]["request_count"])
                    for task in result["tasks"]:
                        self.assertEqual(
                            task["status"] == "completed",
                            task["completion"] is not None and task["abandon_time"] is None,
                        )
                        self.assertEqual(
                            task["status"] == "abandoned",
                            task["abandon_time"] is not None and task["start"] is None,
                        )

    def test_violation_rate_denominator_is_all_input_tasks(self):
        result = _run("data/normal.json", "single")
        metrics = result["metrics"]
        self.assertEqual(metrics["completed_count"] + metrics["abandoned_count"], metrics["request_count"])
        self.assertEqual(
            metrics["violation_rate_exact"],
            str(Fraction(metrics["violated_count"], metrics["request_count"])),
        )
        self.assertEqual(metrics["on_time_count"] + metrics["violated_count"], metrics["request_count"])

    def test_started_batch_runs_late_as_completed_violation(self):
        result = _run("data/normal.json", "batch")
        tasks = {task["id"]: task for task in result["tasks"]}
        # r00 在 deadline 6 之前（3）开批，但仍运行到底，完成 9 记违约而不是放弃。
        self.assertEqual(tasks["r00"]["status"], "completed")
        self.assertEqual(tasks["r00"]["start"], 3)
        self.assertEqual(tasks["r00"]["completion"], 9)
        self.assertTrue(tasks["r00"]["violation"])

    def test_empty_material_has_zero_outcomes_and_full_window_idle(self):
        result = _run("data/empty.json", "single")
        metrics = result["metrics"]
        self.assertEqual(result["tasks"], [])
        self.assertEqual(metrics["completed_count"], 0)
        self.assertEqual(metrics["abandoned_count"], 0)
        self.assertEqual(metrics["queue_area_window"], 0)
        self.assertEqual(metrics["worker_busy_time_window"], 0)
        self.assertEqual(metrics["worker_idle_time_window"], 15)
        self.assertEqual(metrics["cost_window"], str(Fraction(15, 4)))


class WindowCostTests(unittest.TestCase):
    def test_cost_uses_busy_and_idle_fee_table(self):
        result = _run("data/synthetic-same-instant.json", "one-worker")
        metrics = result["metrics"]
        expected = Fraction(6) * Fraction(2) + Fraction(2) * Fraction(1, 2)
        self.assertEqual(metrics["cost_window"], str(expected))
        self.assertEqual(metrics["cost_breakdown"]["busy_time"], 6)
        self.assertEqual(metrics["cost_breakdown"]["idle_time"], 2)

    def test_all_window_crosschecks_hold(self):
        for path in [
            "data/normal.json",
            "data/empty.json",
            "data/synthetic-same-instant.json",
            "data/synthetic-wait-tail.json",
            "data/synthetic-contention.json",
            "data/synthetic-blocking.json",
        ]:
            value = _value(path)
            for result in analyze(value)["configurations"]:
                with self.subTest(path=path, config=result["config"]["id"]):
                    metrics = result["metrics"]
                    self.assertTrue(metrics["queue_area_crosscheck_matches"])
                    self.assertTrue(metrics["worker_busy_crosscheck_matches"])
                    self.assertEqual(
                        metrics["queue_area_window"],
                        metrics["queue_area_window_crosscheck"],
                    )
                    self.assertEqual(
                        metrics["worker_busy_time_window"],
                        metrics["worker_busy_time_window_crosscheck"],
                    )
                    self.assertEqual(
                        metrics["worker_busy_time_window"]
                        + metrics["worker_idle_time_window"],
                        result["config"]["workers"] * metrics["window_length"],
                    )


class ReplayTests(unittest.TestCase):
    def test_same_input_and_config_replays_identically(self):
        value = _value("data/synthetic-wait-tail.json")
        first = simulate(value, "tail-window")
        second = simulate(json.loads(json.dumps(value)), "tail-window")
        self.assertEqual(first, second)

    def test_request_input_order_does_not_change_semantics(self):
        value = _value("data/synthetic-contention.json")
        reordered = dict(value)
        reordered["requests"] = list(reversed(value["requests"]))
        self.assertEqual(analyze(value), analyze(reordered))

    def test_event_sequence_is_monotonic(self):
        for path in [
            "data/normal.json",
            "data/synthetic-same-instant.json",
            "data/synthetic-wait-tail.json",
            "data/synthetic-contention.json",
            "data/synthetic-blocking.json",
        ]:
            for result in analyze(_value(path))["configurations"]:
                events = result["events"]
                self.assertEqual([event["seq"] for event in events], list(range(len(events))))
                self.assertEqual(
                    [event["time"] for event in events],
                    sorted(event["time"] for event in events),
                )


class ValidationTests(unittest.TestCase):
    def test_invalid_deadline_is_rejected(self):
        with self.assertRaises(ValueError):
            validate_input(_value("data/invalid-deadline.json"))

    def test_missing_service_size_for_configured_batch_is_rejected(self):
        value = {
            "requests": [{"id": "r", "arrival": 0, "deadline": 1, "type": "image"}],
            "service": {"image": {"1": 2}},
            "configurations": [{"id": "c", "workers": 1, "max_batch": 2, "wait_window": 0}],
            "observation_window": [0, 5],
            "cost_per_time": {"busy": "1", "idle": "0"},
        }
        with self.assertRaises(ValueError):
            validate_input(value)

    def test_bad_window_and_cost_are_rejected(self):
        value = {
            "requests": [],
            "service": {"image": {"1": 2}},
            "configurations": [{"id": "c", "workers": 1, "max_batch": 1, "wait_window": 0}],
            "observation_window": [5, 4],
            "cost_per_time": {"busy": "-1", "idle": "0"},
        }
        with self.assertRaises(ValueError):
            validate_input(value)

    def test_unknown_config_is_rejected(self):
        with self.assertRaises(ValueError):
            simulate(_value("data/normal.json"), "missing")


if __name__ == "__main__":
    unittest.main()
