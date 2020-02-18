from quickdb.datarake.safeevent import SafeEvent
from typing import Any, Callable, Dict, NamedTuple, Optional


class Progress:
    def __init__(self, total: int, done: int, data: Any = None):
        self.total = total
        self.done = done
        self.data = data

    def _asdict(self):
        return {'total': self.total, 'done': self.done, 'data': self.data}


ProgressCB = Callable[[Progress], None]
RunMakeEnv = Callable[[str, Dict, Optional[ProgressCB], Optional[SafeEvent]], Any]
