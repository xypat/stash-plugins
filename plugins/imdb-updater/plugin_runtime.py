import fcntl
import json
import os
import re
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

LOG_PREFIX = "\x01"
LOG_SUFFIX = "\x02"
LOG_LEVEL_INFO = "i"
LOG_LEVEL_PROGRESS = "p"
LOG_LEVEL_WARN = "w"


def emit_log(level: str, message: str) -> None:
    print(f"{LOG_PREFIX}{level}{LOG_SUFFIX}{message}", file=sys.stderr, flush=True)


def emit_info(message: str) -> None:
    emit_log(LOG_LEVEL_INFO, message)


def emit_progress(progress: float) -> None:
    clamped = max(0.0, min(1.0, progress))
    emit_log(LOG_LEVEL_PROGRESS, f"{clamped:.4f}")


def emit_warn(message: str) -> None:
    emit_log(LOG_LEVEL_WARN, message)


@contextmanager
def exclusive_lock(lock_path: Path) -> Iterator[None]:
    """Let only one instance run at a time.

    A scan is multi-threaded, so several files of one title fire the hook concurrently. Without this
    lock they would all find that the group does not exist yet and each create one. The lock is
    released when the process exits, so it never goes stale.
    """
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("w") as lock_file:
        fcntl.flock(lock_file, fcntl.LOCK_EX)
        yield


def load_plugin_input() -> dict[str, Any]:
    raw = sys.stdin.read().strip()
    return json.loads(raw) if raw else {}


def read_api_key(config_dir: str | None) -> str | None:
    environment_key = os.getenv("STASH_KEY")
    if environment_key:
        return environment_key
    if not config_dir:
        return None

    config_path = Path(config_dir) / "config.yml"
    if not config_path.exists():
        return None

    text = config_path.read_text(encoding="utf-8")
    match = re.search(r"^api_key:\s*(\S+)", text, re.MULTILINE)
    return match.group(1) if match else None


def build_cookie_header(session_cookie: dict[str, Any] | None) -> str | None:
    if not session_cookie:
        return None

    name = session_cookie.get("Name")
    value = session_cookie.get("Value")
    if not name or not value:
        return None

    return f"{name}={value}"
