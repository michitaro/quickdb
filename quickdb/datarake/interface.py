from typing import Any, Callable, Dict, NamedTuple, Optional

class Progress(NamedTuple):
    total: int
    done: int

ProgressCB = Callable[[Progress], None]
RunMakeEnv = Callable[[str, Dict, Optional[ProgressCB]], Any]
