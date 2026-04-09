from __future__ import annotations

import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
PYTHON_PATHS = ["src", "plugins"]


def run_command(command: list[str]) -> int:
    completed = subprocess.run(command, cwd=ROOT, check=False)
    return completed.returncode


def lint_main() -> None:
    commands = [
        ["ruff", "format", "--check", *PYTHON_PATHS],
        ["ruff", "check", *PYTHON_PATHS],
        ["basedpyright", *PYTHON_PATHS],
    ]
    for command in commands:
        if run_command(command) != 0:
            raise SystemExit(1)


def fix_main() -> None:
    commands = [
        ["ruff", "format", *PYTHON_PATHS],
        ["ruff", "check", "--fix", *PYTHON_PATHS],
        ["basedpyright", *PYTHON_PATHS],
    ]
    for command in commands:
        if run_command(command) != 0:
            raise SystemExit(1)


if __name__ == "__main__":
    raise SystemExit("Run one of the console scripts instead: `uv run lint` or `uv run fix`.")
