from datetime import datetime, date, time, timedelta
import gspread
from typing import Dict, List, Optional, Tuple
from .config import HEADER_MAX_COLS, DAY_CACHE_TTL_SEC, OA_PREFIX
from .models import TimeBlock, SheetInfo
from .quotas import first_row, read_cols_exact, read_day_column_map_cached, invalidate_day_cache
from .utils import (
    normalize_day, fmt_time, is_time_token, time_slots,
)
import time as _pytime
from .utils import canon_name_like_cell, fmt_time
from .utils import matches_exact_oa_label
from .quotas import read_day_column_map_cached, invalidate_day_cache

class Schedule:
    def __init__(self, ss: gspread.Spreadsheet):
        self.ss = ss
        self._ws_map: Optional[Dict[str, gspread.Worksheet]] = None
        self._sheet_infos: Dict[str, SheetInfo] = {}

    def _load_ws_map(self):
        if self._ws_map is not None: return
        for i in range(4):
            try:
                sheets = self.ss.worksheets(); break
            except Exception as e:
                if "429" in str(e) or "Quota exceeded" in str(e):
                    _pytime.sleep(0.7 * (2 ** i)); continue
                raise
        else:
            sheets = self.ss.worksheets()
        self._ws_map = {ws.title: ws for ws in sheets}

    def _build_header_map(self, header_row: list[str]) -> Dict[str,int]:
        hdr = {}
        mapping = {
            "mon":"monday","monday":"monday",
            "tue":"tuesday","tues":"tuesday","tuesday":"tuesday",
            "wed":"wednesday","weds":"wednesday","wednesday":"wednesday",
            "thu":"thursday","thur":"thursday","thurs":"thursday","thursday":"thursday",
            "fri":"friday","friday":"friday",
            "sat":"saturday","saturday":"saturday",
            "sun":"sunday","sunday":"sunday",
        }
        for idx, label in enumerate(header_row, start=1):
            lab = (label or "").strip().lower()
            if lab in mapping: hdr[mapping[lab]] = idx
        return hdr

    def _build_sheet_info_lazy(self, ws: gspread.Worksheet) -> SheetInfo:
        header_row = first_row(ws, max_cols=HEADER_MAX_COLS)
        header_map = self._build_header_map(header_row)
        if not header_map:
            raise RuntimeError(f"Could not read weekday header from '{ws.title}'.")
        total_rows = ws.row_count or 2000
        start_r, end_r = (2 if total_rows >= 2 else 1), total_rows
        time_col = read_cols_exact(ws, start_r, end_r, [1])[1]

        blocks_by_time: Dict[time, TimeBlock] = {}
        times_sorted: List[time] = []
        current_time = None
        lane_rows: list[int] = []

        for offset, cell in enumerate(time_col, start=start_r):
            ttok = is_time_token(cell)
            if ttok:
                if current_time is not None:
                    blocks_by_time[current_time] = TimeBlock(current_time, lane_rows[:])
                    times_sorted.append(current_time)
                current_time = ttok; lane_rows = []
            else:
                if current_time is not None:
                    lane_rows.append(offset)
        if current_time is not None:
            blocks_by_time[current_time] = TimeBlock(current_time, lane_rows[:])
            times_sorted.append(current_time)

        lane_rows_all = [r for blk in blocks_by_time.values() for r in blk.lane_rows]
        day_min_row = min(lane_rows_all) if lane_rows_all else start_r
        day_max_row = max(lane_rows_all) if lane_rows_all else end_r

        return SheetInfo(
            ws=ws, header_map=header_map, blocks_by_time=blocks_by_time,
            times_sorted=sorted(set(times_sorted), key=lambda t: (t.hour, t.minute)),
            day_min_row=day_min_row, day_max_row=day_max_row
        )

    def _get_sheet(self, campus: str) -> SheetInfo:
        self._load_ws_map()
        want = campus.strip().lower()
        title = None
        for t in self._ws_map.keys():
            if want == t.lower() or want == t.split()[0].lower(): title = t; break
        if not title: raise ValueError(f"Unknown campus/tab: {campus}")
        if title not in self._sheet_infos:
            self._sheet_infos[title] = self._build_sheet_info_lazy(self._ws_map[title])
        return self._sheet_infos[title]

    # ---- fuzzy fit to ladder (AM/PM flip if needed) ----
    def _coerce_to_ladder_time(self, info: SheetInfo, t: time) -> time:
        if t in info.blocks_by_time: return t
        flipped = time((t.hour + 12) % 24, t.minute)
        if flipped in info.blocks_by_time: return flipped
        ladder = [fmt_time(x) for x in sorted(info.blocks_by_time.keys(), key=lambda x: (x.hour, x.minute))]
        raise ValueError(f"Time {fmt_time(t)} not found in the sheet's time ladder. Try one of: {', '.join(ladder[:8])}...")

    def _blocks_for_range(self, info: SheetInfo, start: time, end: time) -> List[TimeBlock]:
        fitted = [self._coerce_to_ladder_time(info, s) for s in time_slots(start, end)]
        return [info.blocks_by_time[s] for s in fitted]

    def _col_for_day(self, info: SheetInfo, day: str) -> int:
        d = normalize_day(day)
        if d not in info.header_map: raise ValueError(f"Day '{day}' not present in worksheet '{info.ws.title}'.")
        return info.header_map[d]

    def _lane_count_for_blocks(self, blocks: List[TimeBlock]) -> int:
        return min(len(b.lane_rows) for b in blocks) if blocks else 0

    def _read_rows_from_map(self, row_map: Dict[int, str], rows: List[int]) -> List[str]:
        return [row_map.get(r, "") for r in rows]

    def _find_empty_lane_index(self, day_col_map: Dict[int, str], blocks: List[TimeBlock]) -> Optional[int]:
        lane_count = self._lane_count_for_blocks(blocks)
        for lane_idx in range(lane_count):
            rows = [b.lane_rows[lane_idx] for b in blocks]
            vals = self._read_rows_from_map(day_col_map, rows)
            if all(v.strip() == "" or v.strip().lower() in {"oa:", "goa:"} for v in vals):
                return lane_idx
        return None

    def _suggest_next_window(self, info: SheetInfo, col: int, day_col_map: Dict[int, str], start_from: time, needed_slots: int) -> Optional[Tuple[time, time]]:
        times = info.times_sorted
        if not times: return None
        try:
            start_idx = next(i for i,t in enumerate(times) if (t.hour, t.minute) >= (start_from.hour, start_from.minute))
        except StopIteration:
            start_idx = 0
        for j in range(start_idx, len(times) - needed_slots + 1):
            blocks = [info.blocks_by_time[times[j+k]] for k in range(needed_slots)]
            if self._find_empty_lane_index(day_col_map, blocks) is not None:
                s = times[j]; e_dt = datetime.combine(date.today(), s) + timedelta(minutes=30*needed_slots)
                return s, e_dt.time()
        for j in range(0, start_idx):
            if j + needed_slots <= len(times):
                blocks = [info.blocks_by_time[times[j+k]] for k in range(needed_slots)]
                if self._find_empty_lane_index(day_col_map, blocks) is not None:
                    s = times[j]; e_dt = datetime.combine(date.today(), s) + timedelta(minutes=30*needed_slots)
                    return s, e_dt.time()
        return None

    def _batch_write_cells(self, info: SheetInfo, col: int, rows: List[int], values: List[str]):
        data = []
        import gspread.utils as a1
        for r, v in zip(rows, values):
            ref = a1.rowcol_to_a1(r, col)
            data.append({"range": f"{ref}:{ref}", "values": [[v]]})
        info.ws.batch_update(data)

    # ---- public ops (add uses FCFS in app layer) ----
    def add_shift(self, info: SheetInfo, col: int, blocks: List[TimeBlock], name: str) -> None:
        from .config import OA_PREFIX
        day_col_map = read_day_column_map_cached(info, col, ttl_sec=DAY_CACHE_TTL_SEC)
        lane_idx = self._find_empty_lane_index(day_col_map, blocks)
        if lane_idx is None:
            s = blocks[0].time_value; e_dt = datetime.combine(date.today(), s) + timedelta(minutes=30*len(blocks))
            suggestion = self._suggest_next_window(info, col, day_col_map, start_from=s, needed_slots=len(blocks))
            if suggestion:
                raise ValueError(f"No empty lane for whole window. Next empty: {fmt_time(suggestion[0])}–{fmt_time(suggestion[1])}.")
            raise ValueError("No contiguous empty window found today.")
        rows = [b.lane_rows[lane_idx] for b in blocks]
        self._batch_write_cells(info, col, rows, [f"OA: {name}"] * len(rows))  # <--- IMPORTANT
        invalidate_day_cache(info, col)



    def remove_shift(self, info: SheetInfo, col: int, blocks: List[TimeBlock], who_name: str) -> None:
        day_col_map = read_day_column_map_cached(info, col, ttl_sec=DAY_CACHE_TTL_SEC)
        lane_count = self._lane_count_for_blocks(blocks)

        target_lane = None
        for lane_idx in range(lane_count):
            rows = [b.lane_rows[lane_idx] for b in blocks]
            vals = self._read_rows_from_map(day_col_map, rows)
            # All cells in this lane/window must exactly be "OA: <Roster Name>"
            if all(matches_exact_oa_label(v, who_name) for v in vals):
                target_lane = lane_idx
                break

        if target_lane is None:
            raise ValueError(
                "These slots are not fully assigned to you. If you just added this, try again, "
                "and make sure the sidebar name exactly matches the roster (including spacing)."
            )

        rows = [b.lane_rows[target_lane] for b in blocks]
        self._batch_write_cells(info, col, rows, [""] * len(rows))
        invalidate_day_cache(info, col)