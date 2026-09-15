from __future__ import annotations

from pathlib import Path
import sys
from tempfile import TemporaryDirectory
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from instagram_collector.export_organizer import organize_exports


class ExportOrganizerTests(unittest.TestCase):
    def test_dry_run_does_not_change_files(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "instagram_09-09-26" / "Candidate" / "stories" / "2026-09-09" / "story.jpg"
            source.parent.mkdir(parents=True)
            source.write_bytes(b"story")

            result = organize_exports(root)

            self.assertEqual(result.planned, 1)
            self.assertTrue(source.exists())
            self.assertFalse((root / "instagram").exists())

    def test_apply_merges_json_media_and_stories_by_candidate(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            period = root / "instagram_01-09-26-to-09-09-26" / "Candidate"
            day = root / "instagram_09-09-26" / "Candidate"
            files = {
                period / "09092026.json": b"json",
                period / "media" / "2026-09-09" / "CODE" / "01_image.jpg": b"post",
                day / "stories" / "2026-09-09" / "instagram" / "candidate" / "story.mp4": b"story",
            }
            for path, content in files.items():
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(content)

            result = organize_exports(root, apply=True)

            self.assertEqual((result.planned, result.moved, len(result.conflicts)), (3, 3, 0))
            target = root / "instagram" / "Candidate"
            self.assertEqual((target / "09092026.json").read_bytes(), b"json")
            self.assertEqual((target / "media" / "2026-09-09" / "CODE" / "01_image.jpg").read_bytes(), b"post")
            self.assertEqual(
                (target / "stories" / "2026-09-09" / "story.mp4").read_bytes(),
                b"story",
            )
            self.assertFalse(period.parents[0].exists())
            self.assertFalse(day.parents[0].exists())

    def test_existing_destination_is_never_overwritten(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "instagram_09-09-26" / "Candidate" / "09092026.json"
            destination = root / "instagram" / "Candidate" / "09092026.json"
            source.parent.mkdir(parents=True)
            destination.parent.mkdir(parents=True)
            source.write_bytes(b"source")
            destination.write_bytes(b"destination")

            result = organize_exports(root, apply=True)

            self.assertEqual(len(result.conflicts), 1)
            self.assertEqual(destination.read_bytes(), b"destination")
            self.assertEqual(source.read_bytes(), b"source")

    def test_existing_consolidated_story_tree_is_flattened(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "instagram" / "Candidate" / "stories" / "2026-09-09" / "instagram" / "candidate" / "story.jpg"
            source.parent.mkdir(parents=True)
            source.write_bytes(b"story")

            result = organize_exports(root, apply=True)

            destination = root / "instagram" / "Candidate" / "stories" / "2026-09-09" / "story.jpg"
            self.assertEqual((result.planned, result.moved), (1, 1))
            self.assertEqual(destination.read_bytes(), b"story")
            self.assertFalse((destination.parent / "instagram").exists())
