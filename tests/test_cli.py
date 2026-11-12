"""核对命令行输出保留输入指纹且不覆盖原始资料。"""
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from analysis_data.io import load

ROOT = Path(__file__).resolve().parents[1]


class CliTests(unittest.TestCase):
    def _run(self, *args):
        return subprocess.run(
            [sys.executable, "-m", "analysis_data.cli", *args],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
        )

    def test_compare_keeps_source_fingerprint(self):
        completed = self._run("compare", "data/normal.json")
        self.assertEqual(completed.returncode, 0, completed.stderr)
        output = json.loads(completed.stdout)
        digest = load(ROOT / "data/normal.json")[1]
        self.assertEqual(output["source_sha256"], digest)
        self.assertEqual(len(output["configurations"]), 2)
        self.assertEqual(output["comparison"][0]["queue_area_window"], 19)

    def test_output_file_is_written_and_input_is_protected(self):
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "result.json"
            completed = self._run("compare", "data/normal.json", "--output", str(target))
            self.assertEqual(completed.returncode, 0, completed.stderr)
            self.assertTrue(target.exists())
            self.assertEqual(json.loads(target.read_text(encoding="utf8")), json.loads(completed.stdout))

            overwrite = self._run("compare", "data/normal.json", "--output", "data/normal.json")
            self.assertNotEqual(overwrite.returncode, 0)
            self.assertIn("不能覆盖输入资料", overwrite.stderr)

    def test_simulate_selected_config(self):
        completed = self._run("simulate", "data/normal.json", "--config", "batch")
        self.assertEqual(completed.returncode, 0, completed.stderr)
        output = json.loads(completed.stdout)
        self.assertEqual([row["config"]["id"] for row in output["configurations"]], ["batch"])


if __name__ == "__main__":
    unittest.main()
