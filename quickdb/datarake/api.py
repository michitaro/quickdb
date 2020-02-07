from quickdb.datarake.interface import Progress
from typing import Any, Dict, NamedTuple


class WorkerRequest(NamedTuple):
    make_env: str
    shared: Dict


class WorkerResult(NamedTuple):
    value: Any


class UserError(NamedTuple):
    reason: str
