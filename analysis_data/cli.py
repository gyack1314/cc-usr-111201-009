"""命令行入口：只读原始资料，输出可复演的摘要或模拟结果。"""
import argparse
import json
from pathlib import Path

from .domain import summarize
from .io import load
from .simulation import analyze


def _write_output(value, path=None, source_path=None):
    text = json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False)
    if path is not None:
        output = Path(path)
        if source_path is not None and output.resolve() == Path(source_path).resolve():
            raise ValueError("输出路径不能覆盖输入资料")
        output.write_text(text + "\n", encoding="utf8")
    print(text)


def _build_parser():
    parser = argparse.ArgumentParser(prog="analysis_data")
    subparsers = parser.add_subparsers(dest="command", required=True)

    inspect_parser = subparsers.add_parser("inspect", help="查看原始资料摘要")
    inspect_parser.add_argument("file")
    inspect_parser.add_argument("--output", help="将结果写入此文件，不能是输入文件")

    simulate_parser = subparsers.add_parser("simulate", help="运行一个或多个配置")
    simulate_parser.add_argument("file")
    simulate_parser.add_argument(
        "--config",
        action="append",
        dest="configs",
        help="只运行指定配置，可重复给出；默认运行全部",
    )
    simulate_parser.add_argument("--output", help="将结果写入此文件，不能是输入文件")

    compare_parser = subparsers.add_parser("compare", help="在同一日志上比较全部配置")
    compare_parser.add_argument("file")
    compare_parser.add_argument("--output", help="将结果写入此文件，不能是输入文件")

    analyze_parser = subparsers.add_parser("analyze", help="compare 的别名")
    analyze_parser.add_argument("file")
    analyze_parser.add_argument("--output", help="将结果写入此文件，不能是输入文件")

    capacity_parser = subparsers.add_parser("capacity", help="compare 的别名")
    capacity_parser.add_argument("file")
    capacity_parser.add_argument("--output", help="将结果写入此文件，不能是输入文件")

    return parser


def main():
    args = _build_parser().parse_args()
    value, digest = load(args.file)

    if args.command == "inspect":
        _write_output(
            {"source_sha256": digest, "summary": summarize(value)},
            getattr(args, "output", None),
            args.file,
        )
        return

    config_ids = getattr(args, "configs", None)
    result = analyze(value, config_ids=config_ids, source_sha256=digest)
    _write_output(result, getattr(args, "output", None), args.file)


if __name__ == "__main__":
    main()
