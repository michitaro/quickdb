from typing import Dict, Tuple, Type

from quickdb.sql2mapreduce.agg import AggCall

from .count import CoutnAggCall
from .histogram import HistogramAggCall

agg_functions: Dict[Tuple[str, ...], Type[AggCall]] = {
    ('count', ): CoutnAggCall,
    ('histogram', ): HistogramAggCall,
}
