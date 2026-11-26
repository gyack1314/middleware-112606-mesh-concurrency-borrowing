"""Conventional import alias / CLI entry: ``python service.py ...``."""

from admission_service import *  # noqa: F401,F403
from admission_service import main

if __name__ == "__main__":
    raise SystemExit(main())
