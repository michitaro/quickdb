from typing import Any, Dict, NamedTuple


class WorkerRequest(NamedTuple):
    make_env: str
    shared: Dict


class WorkerResult(NamedTuple):
    value: Any


class UserError(NamedTuple):
    reason: str


class Progress(NamedTuple):
    value: float
