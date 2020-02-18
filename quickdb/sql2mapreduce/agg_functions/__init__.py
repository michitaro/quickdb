from quickdb.sql2mapreduce.agg_functions.sum import SumAggCall
from quickdb.sql2mapreduce.agg_functions.crossmatch import CrossMatchAggCall
from typing import Dict, Tuple, Type

from quickdb.sql2mapreduce.agg import AggCall
from quickdb.sql2mapreduce.agg_functions.histogram2d import HistogramAgg2DCall
from quickdb.sql2mapreduce.agg_functions.sleep import SleepAggCall

from .count import CountAggCall
from .histogram import HistogramAggCall
from .minmax import MaxAggCall, MinAggCall, MinMaxAggCall

agg_functions: Dict[Tuple[str, ...], Type[AggCall]] = {
    ('count', ): CountAggCall,
    ('sum', ): SumAggCall,
    ('min', ): MinAggCall,
    ('max', ): MaxAggCall,
    ('minmax', ): MinMaxAggCall,
    ('histogram', ): HistogramAggCall,
    ('histogram2d', ): HistogramAgg2DCall,
    ('crossmatch',): CrossMatchAggCall,
    ('sleep', ): SleepAggCall
}
