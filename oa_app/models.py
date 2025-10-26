from dataclasses import dataclass
from datetime import time
import gspread
from typing import Dict, List

@dataclass
class TimeBlock:
    time_value: time
    lane_rows: List[int]

@dataclass
class SheetInfo:
    ws: gspread.Worksheet
    header_map: Dict[str, int]
    blocks_by_time: Dict[time, TimeBlock]
    times_sorted: List[time]
    day_min_row: int
    day_max_row: int
