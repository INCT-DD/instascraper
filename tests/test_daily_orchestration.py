from __future__ import annotations

import os
from pathlib import Path
import stat
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "run_daily_cron.sh"


class DailyOrchestrationTests(unittest.TestCase):
    def _run_script(self, collection_exit: int = 0, worker_exit: int = 0) -> tuple[subprocess.CompletedProcess[str], list[str]]:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            call_log = tmp_path / "calls.log"
            fake_docker = tmp_path / "docker"
            fake_docker.write_text(
                "#!/bin/sh\n"
                "printf '%s\n' \"$*\" >> \"$CALL_LOG\"\n"
                "case \"$*\" in\n"
                "  'compose run --rm app python -m pipeline run-scheduled --skip-jobs --export --date 2026-09-15') exit \"$COLLECTION_EXIT\" ;;\n"
                "  'compose up -d media-worker') exit \"$WORKER_EXIT\" ;;\n"
                "  *) exit 0 ;;\n"
                "esac\n",
                encoding="utf-8",
            )
            fake_docker.chmod(fake_docker.stat().st_mode | stat.S_IXUSR)
            env = {
                **os.environ,
                "PATH": f"{tmp_path}{os.pathsep}{os.environ.get('PATH', '')}",
                "CALL_LOG": str(call_log),
                "COLLECTION_EXIT": str(collection_exit),
                "WORKER_EXIT": str(worker_exit),
            }
            completed = subprocess.run(
                ["bash", str(SCRIPT), "--date", "2026-09-15"],
                cwd=ROOT,
                env=env,
                text=True,
                capture_output=True,
                check=False,
            )
            calls = call_log.read_text(encoding="utf-8").splitlines()
        return completed, calls

    def test_collection_finishes_before_worker_starts(self) -> None:
        completed, calls = self._run_script()

        self.assertEqual(completed.returncode, 0)
        self.assertEqual(calls, [
            "compose stop media-worker",
            "compose run --rm app python -m pipeline run-scheduled --skip-jobs --export --date 2026-09-15",
            "compose up -d media-worker",
        ])

    def test_partial_collection_still_starts_worker_and_preserves_exit_code(self) -> None:
        completed, calls = self._run_script(collection_exit=1)

        self.assertEqual(completed.returncode, 1)
        self.assertEqual(calls[-1], "compose up -d media-worker")

    def test_worker_start_failure_has_precedence(self) -> None:
        completed, calls = self._run_script(worker_exit=7)

        self.assertEqual(completed.returncode, 7)
        self.assertEqual(calls[-1], "compose up -d media-worker")
        self.assertIn("Failed to start media-worker", completed.stderr)


if __name__ == "__main__":
    unittest.main()
