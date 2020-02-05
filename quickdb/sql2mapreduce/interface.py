from typing import Any, Callable, Dict, Optional

ProgressCB = Callable[[float], None]
RunMakeEnv = Callable[[str, Dict, Optional[ProgressCB]], Any]
