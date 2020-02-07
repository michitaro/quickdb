from typing import Any, Iterable, List, Optional, Tuple, Union, overload


class _dtype:
    kind: str


def dtype(typename: str) -> _dtype: ...


class ndarray:
    @overload
    def __getitem__(self, slice: Union['ndarray', slice]) -> 'ndarray': ...
    def __getitem__(self, index: int) -> ...: ...

    def __len__(self) -> int: ...

    def __eq__(self, other) -> 'ndarray': ...
    def __ne__(self, other) -> 'ndarray': ...
    def __lt__(self, other) -> 'ndarray': ...
    def __le__(self, other) -> 'ndarray': ...
    def __gt__(self, other) -> 'ndarray': ...
    def __ge__(self, other) -> 'ndarray': ...

    def __add__(self, other) -> 'ndarray': ...
    def __sub__(self, other) -> 'ndarray': ...
    def __mul__(self, other) -> 'ndarray': ...
    def __div__(self, other) -> 'ndarray': ...
    def __mod__(self, other) -> 'ndarray': ...

    def __radd__(self, other) -> 'ndarray': ...
    def __rsub__(self, other) -> 'ndarray': ...
    def __rmul__(self, other) -> 'ndarray': ...
    def __rdiv__(self, other) -> 'ndarray': ...
    def __rmod__(self, other) -> 'ndarray': ...

    def __pos__(self) -> 'ndarray': ...
    def __neg__(self) -> 'ndarray': ...

    def fill(self, value) -> None: ...
    def all(self) -> bool: ...
    def sum(self, *args, **kwargs): ...

    def __iter__(self) -> ...: ...

    shape: Tuple[int, ...]

    dtype: _dtype

    @property
    def T(self) -> 'ndarray': ...


def array(*args, **kwargs) -> ndarray: ...


def concatenate(arrays: Tuple[ndarray, ...]) -> ndarray: ...


def lexsort(a: List[ndarray]) -> ndarray: ...


def arange(start: int, stop: int = None, step: int = None, **kwargs) -> ndarray: ...


def nonzero(a: ndarray) -> Tuple[ndarray, ...]: ...


def empty(shape: Iterable[int], dtype: Union[_dtype, str] = None) -> ndarray: ...


def load(filename: str) -> ndarray: ...


def allclose(a: ndarray, b: ndarray, equal_nan: bool = None) -> bool: ...


def logical_and(a: ndarray, b: ndarray) -> ndarray: ...


def logical_or(a: ndarray, b: ndarray) -> ndarray: ...


def logical_not(a: ndarray) -> ndarray: ...


def isnan(a: ndarray) -> ndarray: ...


def isfinite(a: ndarray) -> ndarray: ...


def unique(a: ndarray, *args, **kwargs) -> ...: ...


def histogram(a: ndarray, bins=10, range=None, normed=None, weights=None, density=None) -> Tuple[ndarray, ndarray]: ...


def histogram2d(*args, **kwargs) -> Tuple[ndarray, ndarray]: ...


def min(*args, **kwargs): ...


def max(*args, **kwargs): ...


def nanmin(*args, **kwargs): ...


def nanmax(*args, **kwargs): ...


def log10(*args, **kwargs) -> ndarray: ...


@overload
def percentile(a: ndarray, q: float) -> float: ...
@overload
def percentile(a: ndarray, q: ndarray) -> ndarray: ...


def seterr(*args, **kwargs): ...


nan: float
