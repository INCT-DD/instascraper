from __future__ import annotations

import argparse
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from instagram_collector.export_organizer import organize_exports


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Merge dated Instagram exports into one candidate-oriented directory.",
    )
    parser.add_argument("root", type=Path, help="Directory containing instagram_* export folders.")
    parser.add_argument("--target-name", default="instagram", help="Consolidated folder name inside root.")
    parser.add_argument("--apply", action="store_true", help="Move files. Without this flag, only simulate.")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    result = organize_exports(args.root, apply=args.apply, target_name=args.target_name)
    mode = "APPLY" if args.apply else "DRY-RUN"
    print(f"Mode: {mode}")
    print(f"Dated source folders: {len(result.sources)}")
    print(f"Files eligible to move: {result.planned}")
    print(f"Files moved: {result.moved}")
    print(f"Conflicts preserved at source: {len(result.conflicts)}")
    print(f"Move errors: {len(result.errors)}")
    print(f"Empty directories removed: {result.directories_removed}")
    for conflict in result.conflicts[:20]:
        print(f"CONFLICT: {conflict.source} -> {conflict.destination}")
    for error in result.errors[:20]:
        print(f"ERROR: {error}")
    if result.conflicts or result.errors:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
