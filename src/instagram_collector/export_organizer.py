from __future__ import annotations

import re
import shutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable


DATED_EXPORT_PATTERN = re.compile(r"^instagram_\d{2}-\d{2}-\d{2}(?:-to-\d{2}-\d{2}-\d{2})?$")


@dataclass(frozen=True)
class FileMove:
    source: Path
    destination: Path


@dataclass
class OrganizationResult:
    sources: list[Path] = field(default_factory=list)
    planned: int = 0
    moved: int = 0
    conflicts: list[FileMove] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    directories_removed: int = 0


def dated_export_directories(root: Path) -> list[Path]:
    return sorted(
        path
        for path in root.iterdir()
        if path.is_dir() and DATED_EXPORT_PATTERN.fullmatch(path.name)
    )


def plan_moves(root: Path, target_name: str = "instagram") -> tuple[list[Path], list[FileMove], list[FileMove]]:
    target = root / target_name
    sources = dated_export_directories(root)
    moves: list[FileMove] = []
    conflicts: list[FileMove] = []
    destinations: set[Path] = set()
    for source_root in sources:
        for source in _files(source_root):
            destination = target / _flatten_story_relative_path(source.relative_to(source_root))
            operation = FileMove(source, destination)
            if destination.exists() or destination in destinations:
                conflicts.append(operation)
            else:
                moves.append(operation)
                destinations.add(destination)
    if target.is_dir():
        for source in _files(target):
            relative = source.relative_to(target)
            flattened = _flatten_story_relative_path(relative)
            if flattened == relative:
                continue
            destination = target / flattened
            operation = FileMove(source, destination)
            if destination.exists() or destination in destinations:
                conflicts.append(operation)
            else:
                moves.append(operation)
                destinations.add(destination)
    return sources, moves, conflicts


def organize_exports(root: Path, apply: bool = False, target_name: str = "instagram") -> OrganizationResult:
    root = root.resolve()
    if not root.is_dir():
        raise FileNotFoundError(f"Export root does not exist: {root}")

    sources, moves, conflicts = plan_moves(root, target_name)
    result = OrganizationResult(sources=sources, planned=len(moves), conflicts=conflicts)
    if not apply:
        return result

    for operation in moves:
        try:
            operation.destination.parent.mkdir(parents=True, exist_ok=True)
            if operation.destination.exists():
                result.conflicts.append(operation)
                continue
            shutil.move(str(operation.source), str(operation.destination))
            result.moved += 1
        except OSError as exc:
            result.errors.append(f"{operation.source}: {exc}")

    for source_root in sources:
        result.directories_removed += _remove_empty_directories(source_root)
    target = root / target_name
    if target.is_dir():
        result.directories_removed += _remove_empty_directories(target)
    return result


def _files(root: Path) -> Iterable[Path]:
    return (path for path in root.rglob("*") if path.is_file())


def _flatten_story_relative_path(relative: Path) -> Path:
    parts = relative.parts
    if len(parts) > 4 and parts[1] == "stories":
        return Path(parts[0], parts[1], parts[2], parts[-1])
    return relative


def _remove_empty_directories(root: Path) -> int:
    removed = 0
    directories = sorted(
        (path for path in root.rglob("*") if path.is_dir()),
        key=lambda path: len(path.parts),
        reverse=True,
    )
    directories.append(root)
    for directory in directories:
        try:
            directory.rmdir()
            removed += 1
        except OSError:
            pass
    return removed
