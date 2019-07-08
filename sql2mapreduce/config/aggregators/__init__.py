from .count import CountAggregator
from .minmax import MinMaxAggregator
from .histogram import HistogramAggregator
from .histogram2d import Histogram2dAggregator
from .crossmatch import CrossmatchAggregator
from .pickone import PickOneAggregator


mapping = {
    ('count',): CountAggregator,
    ('minmax',): MinMaxAggregator,
    ('histogram',): HistogramAggregator,
    ('histogram2d',): Histogram2dAggregator,
    ('crossmatch',): CrossmatchAggregator,
    (PickOneAggregator,): PickOneAggregator,
}
