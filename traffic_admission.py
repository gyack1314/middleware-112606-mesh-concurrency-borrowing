"""Readable alias for workbench integrations."""

from admission_service import *  # noqa: F401,F403
from admission_service import (  # noqa: F401
    AdmissionService,
    main,
    replay_jsonl,
)

TrafficAdmissionService = AdmissionService

if __name__ == "__main__":
    raise SystemExit(main())
