import time as _pytime
import gspread
import streamlit as st
import gspread.utils as a1

def _safe_batch_get(ws, ranges, *, retries: int = 4, backoff: float = 0.7):
    # self-initialize cache dict
    cache = st.session_state.setdefault("WS_RANGE_CACHE", {})
    key = (getattr(ws, "id", ws.title), tuple(ranges))
    if key in cache:
        return cache[key]
    for i in range(retries):
        try:
            vals = ws.batch_get(ranges, major_dimension="ROWS")
            cache[key] = vals
            return vals
        except Exception as e:
            if "429" in str(e) or "Quota exceeded" in str(e):
                _pytime.sleep(backoff * (2 ** i)); continue
            raise
    vals = ws.batch_get(ranges, major_dimension="ROWS")
    cache[key] = vals
    return vals

def read_cols_exact(ws, start_row: int, end_row: int, col_indices: list[int]) -> dict[int, list[str]]:
    if end_row < start_row:
        return {c: [] for c in col_indices}
    a1s = [f"{a1.rowcol_to_a1(start_row,c)}:{a1.rowcol_to_a1(end_row,c)}" for c in col_indices]
    blocks = _safe_batch_get(ws, a1s)
    need = end_row - start_row + 1
    out = {}
    for c, block in zip(col_indices, blocks):
        col_vals = [r[0] if r else "" for r in (block or [])]
        if len(col_vals) < need: col_vals += [""] * (need - len(col_vals))
        out[c] = col_vals[:need]
    return out

def first_row(ws, max_cols: int) -> list[str]:
    end_a1 = a1.rowcol_to_a1(1, max_cols)
    block = _safe_batch_get(ws, [f"A1:{end_a1}"])[0]
    return block[0] if block else []

def read_day_column_map_cached(info, col: int, ttl_sec: int):
    # self-initialize day cache
    day_cache = st.session_state.setdefault("DAY_CACHE", {})
    ws_id = getattr(info.ws, "id", info.ws.title)
    key = (ws_id, col, info.day_min_row, info.day_max_row)
    now = _pytime.time()
    entry = day_cache.get(key)
    if entry and (now - entry[0]) <= ttl_sec:
        return entry[1]
    rng = f"{a1.rowcol_to_a1(info.day_min_row,col)}:{a1.rowcol_to_a1(info.day_max_row,col)}"
    block = _safe_batch_get(info.ws, [rng])[0]
    flat = [(r[0] if r and len(r)>0 else "") for r in (block or [])]
    mapping = {info.day_min_row + i: v for i, v in enumerate(flat)}
    day_cache[key] = (now, mapping)
    return mapping

def invalidate_day_cache(info, col: int):
    day_cache = st.session_state.setdefault("DAY_CACHE", {})
    ws_id = getattr(info.ws, "id", info.ws.title)
    key = (ws_id, col, info.day_min_row, info.day_max_row)
    day_cache.pop(key, None)
