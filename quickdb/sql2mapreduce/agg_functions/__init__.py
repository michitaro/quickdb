from typing import Dict, Tuple, Type

from quickdb.sql2mapreduce.agg import AggCall

from .count import CountAggCall
from .histogram import HistogramAggCall
from .minmax import MaxAggCall, MinAggCall

agg_functions: Dict[Tuple[str, ...], Type[AggCall]] = {
    ('count', ): CountAggCall,
    ('min', ): MinAggCall,
    ('max', ): MaxAggCall,
    ('histogram', ): HistogramAggCall,
}
