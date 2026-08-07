from __future__ import annotations

import importlib.util
from pathlib import Path


_ROOT_MODULE = Path(__file__).resolve().parents[1] / "instagram_scraper.py"
_SPEC = importlib.util.spec_from_file_location("_legacy_instagram_scraper", _ROOT_MODULE)
if _SPEC is None or _SPEC.loader is None:
    raise ImportError(f"Could not load legacy scraper module at {_ROOT_MODULE}")

_module = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_module)

for _name in dir(_module):
    if not _name.startswith("__"):
        globals()[_name] = getattr(_module, _name)
