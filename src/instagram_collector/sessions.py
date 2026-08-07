from __future__ import annotations

import hashlib
from dataclasses import dataclass
from itertools import cycle
from typing import Dict, Iterable, List, Optional


@dataclass(frozen=True)
class CollectorSession:
    name: str
    instagram_cookie_json: str
    gallery_dl_cookies: str

    @property
    def alias(self) -> str:
        digest = hashlib.sha256(self.name.encode("utf-8")).hexdigest()[:8]
        return f"session-{digest}"


class SessionPool:
    def __init__(self, sessions: Iterable[Dict[str, object]], rotation_enabled: bool) -> None:
        self.sessions: List[CollectorSession] = [
            CollectorSession(
                name=str(item.get("name") or f"session-{index + 1}"),
                instagram_cookie_json=str(item.get("instagram_cookie_json") or "cookie.json"),
                gallery_dl_cookies=str(item.get("gallery_dl_cookies") or "cookies.txt"),
            )
            for index, item in enumerate(sessions)
            if bool(item.get("active", True))
        ]
        if not self.sessions:
            raise ValueError("No active collector sessions configured.")
        self.rotation_enabled = rotation_enabled
        self._cycle = cycle(self.sessions)

    def next(self) -> CollectorSession:
        if self.rotation_enabled:
            return next(self._cycle)
        return self.sessions[0]

    def alternatives(self, failed: Optional[CollectorSession] = None) -> List[CollectorSession]:
        if not self.rotation_enabled:
            return []
        failed_alias = failed.alias if failed else None
        return [session for session in self.sessions if session.alias != failed_alias]
