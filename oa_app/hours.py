
from __future__ import annotations
import os
import re
from typing import Iterable, List, Optional, Tuple, Dict

import streamlit as st
import gspread
import gspread.utils as a1

from .config import (
    OA_SCHEDULE_SHEETS,   # ["UNH ...", "MC ..."]
    AUDIT_SHEET,
    LOCKS_SHEET,
    ONCALL_MAX_COLS,
    ONCALL_MAX_ROWS,
    DAY_CACHE_TTL_SEC,
    ONCALL_SHEET_OVERRIDE,  # optional override of On-Call tab name
)
from .quotas import _safe_batch_get, read_day_column_map_cached


# ──────────────────────────────────────────────────────────────────────────────
# Debug controls (no-UI): secrets/env/session_state
# ──────────────────────────────────────────────────────────────────────────────

def _hours_debug_enabled() -> bool:
    """Return True if the caller explicitly enabled slow/verbose counting mode.
    This only affects *how* we count UNH/MC (fast vs grid), not the resulting totals.
    """
    try:
        if bool(st.session_state.get("HOURS_DEBUG")):
            return True
    except Exception:
        pass
    if str(os.environ.get("HOURS_DEBUG", "")).strip() not in ("", "0", "false", "False"):
        return True
    try:
        return bool(st.secrets.get("hours_debug", False))
    except Exception:
        return False


# ──────────────────────────────────────────────────────────────────────────────
# Resolve the three tabs we total over: UNH, MC, and On-Call (neighbor to MC)
# ──────────────────────────────────────────────────────────────────────────────

_DENY_LOW = {AUDIT_SHEET.strip().lower(), LOCKS_SHEET.strip().lower()}

def _resolve_title(actuals: List[gspread.Worksheet], wanted: str) -> str | None:
    wanted_low = (wanted or "").strip().lower()
    by_low = {w.title.strip().lower(): w.title for w in actuals}
    if wanted_low in by_low:
        return by_low[wanted_low]
    first = wanted_low.split()[0] if wanted_low else ""
    for w in actuals:
        t = w.title.strip(); tl = t.lower()
        if tl == wanted_low or (first and tl.startswith(first)):
            return t
    return None

def _three_titles_unh_mc_oncall(ss: gspread.Spreadsheet) -> list[str]:
    try:
        ws_list = ss.worksheets()
    except Exception:
        return []

    unh_cfg, mc_cfg = OA_SCHEDULE_SHEETS[0], OA_SCHEDULE_SHEETS[1]
    unh_title = _resolve_title(ws_list, unh_cfg)
    mc_title  = _resolve_title(ws_list, mc_cfg)

    out: list[str] = []
    if unh_title:
        out.append(unh_title)
    if mc_title:
        out.append(mc_title)

    # Prefer explicit override; else pick the physical neighbor to MC’s right.
    oncall_title = None
    if mc_title:
        if ONCALL_SHEET_OVERRIDE and ONCALL_SHEET_OVERRIDE.strip():
            cand = _resolve_title(ws_list, ONCALL_SHEET_OVERRIDE)
            if cand:
                oncall_title = cand
        else:
            try:
                idx = next(i for i, w in enumerate(ws_list) if w.title == mc_title)
            except StopIteration:
                idx = -1
            if idx >= 0:
                j = idx + 1
                while j < len(ws_list):
                    cand = ws_list[j].title
                    if cand.strip().lower() not in _DENY_LOW:
                        oncall_title = cand
                        break
                    j += 1
    if oncall_title:
        out.append(oncall_title)

    # de-dup
    seen, final = set(), []
    for t in out:
        if t and t not in seen:
            seen.add(t); final.append(t)
    return final


# ──────────────────────────────────────────────────────────────────────────────
# Name matching (handles "OA: Name", "GOA: Name", "Name1 & Name2")
# ──────────────────────────────────────────────────────────────────────────────

_SPLIT_RE = re.compile(r"[,\n/&+]|(?:\s+\band\b\s+)", re.I)
_PREFIX_RE = re.compile(r"^\s*(?:OA|GOA|On[-\s]*Call)\s*:\s*", re.I)

def _canon(s: str) -> str:
    s = _PREFIX_RE.sub("", s or "")
    return " ".join("".join(ch for ch in s.lower() if ch.isalnum() or ch.isspace()).split())

def _cell_mentions_person(cell_value: str, canon_name: str) -> bool:
    if not cell_value:
        return False
    target = _canon(canon_name)
    if _canon(cell_value) == target:
        return True
    parts: Iterable[str] = (p.strip() for p in _SPLIT_RE.split(str(cell_value)) if p.strip())
    return any(_canon(p) == target for p in parts)


# ──────────────────────────────────────────────────────────────────────────────
# UNH/MC: generic half-hour grid counter (0.5h per matched cell)
# ──────────────────────────────────────────────────────────────────────────────

def _count_half_hour_grid(ws: gspread.Worksheet, canon_name: str) -> float:
    end_col_letter = a1.rowcol_to_a1(1, ONCALL_MAX_COLS).split("1")[0]
    values = _safe_batch_get(ws, [f"A1:{end_col_letter}{ONCALL_MAX_ROWS}"])[0] or []
    total = 0.0
    for row in values:
        for cell in (row or []):
            if _cell_mentions_person(str(cell), canon_name):
                total += 0.5
    return total


# ──────────────────────────────────────────────────────────────────────────────
# On-Call by day headers (Mon–Fri=5h, Sat/Sun=4h, unknown→assume weekday=5h)
# ──────────────────────────────────────────────────────────────────────────────

_DAY_ALIASES = {
    "monday": "monday", "mon": "monday",
    "tuesday": "tuesday", "tue": "tuesday", "tues": "tuesday",
    "wednesday": "wednesday", "wed": "wednesday",
    "thursday": "thursday", "thu": "thursday", "thur": "thursday", "thurs": "thursday",
    "friday": "friday", "fri": "friday",
    "saturday": "saturday", "sat": "saturday",
    "sunday": "sunday", "sun": "sunday",
}
_WEEKDAYS = {"monday","tuesday","wednesday","thursday","friday"}

def _normalize_day(s: str) -> Optional[str]:
    s = (s or "").strip().lower()
    s_clean = "".join(ch for ch in s if ch.isalpha() or ch.isspace())
    tokens = {tok for tok in s_clean.split() if tok}
    for tok in list(tokens):
        if tok in _DAY_ALIASES:
            return _DAY_ALIASES[tok]
    return None

def _find_header_row_with_days(values: List[List[str]], max_scan_rows: int = 10) -> Tuple[Optional[int], Dict[int, str]]:
    rows_to_scan = values[:max_scan_rows]
    for r, row in enumerate(rows_to_scan):
        colmap: Dict[int, str] = {}
        hits = 0
        for c, cell in enumerate(row or []):
            day = _normalize_day(str(cell))
            if day:
                colmap[c] = day
                hits += 1
        if hits >= 2:
            return r, colmap
    return None, {}

def _count_oncall_by_day_headers(ws: gspread.Worksheet, canon_name: str) -> float:
    end_col_letter = a1.rowcol_to_a1(1, ONCALL_MAX_COLS).split("1")[0]
    grid = _safe_batch_get(ws, [f"A1:{end_col_letter}{ONCALL_MAX_ROWS}"])[0] or []

    header_r, day_by_col = _find_header_row_with_days(grid)

    def weight_for_col(cidx: int) -> float:
        day = day_by_col.get(cidx)
        if not day:
            return 5.0  # missing/unknown header → weekday weight
        return 5.0 if day in _WEEKDAYS else 4.0

    total = 0.0
    # If no header: every mention gets 5h
    if header_r is None:
        for row in grid:
            for cell in (row or []):
                if _cell_mentions_person(str(cell), canon_name):
                    total += 5.0
        return total

    # With header: count below it using column-specific weights
    for r in range(header_r + 1, len(grid)):
        row = grid[r] or []
        for c, cell in enumerate(row, start=1):
            if _cell_mentions_person(str(cell), canon_name):
                total += weight_for_col(c - 1)
    return total


# ──────────────────────────────────────────────────────────────────────────────
# Cache-busting for strict recomputes
# ──────────────────────────────────────────────────────────────────────────────

def _clear_ws_cache_for_titles(ss: gspread.Spreadsheet, schedule, titles: list[str]) -> None:
    cache = st.session_state.setdefault("WS_RANGE_CACHE", {})
    ws_ids = set()
    for t in titles:
        try:
            info = schedule._get_sheet(t)
            ws_ids.add(getattr(info.ws, "id", info.ws.title))
        except Exception:
            try:
                ws = ss.worksheet(t)
                ws_ids.add(getattr(ws, "id", ws.title))
            except Exception:
                pass
    for key in list(cache.keys()):
        ws_id, _ranges = key
        if ws_id in ws_ids:
            cache.pop(key, None)


# ──────────────────────────────────────────────────────────────────────────────
# EXPORTED API (unchanged signatures)
# ──────────────────────────────────────────────────────────────────────────────

@st.cache_data(show_spinner=False)
def compute_hours_fast(_ss, _schedule, canon_name: str, epoch: int) -> float:
    """
    Cached sidebar metric. Returns **capped** total (max 20).
    UNH + MC: 0.5h per cell
    On-Call: 5h per mention in Mon–Fri columns; 4h in Sat/Sun; if no header → 5h.
    """
    titles = _three_titles_unh_mc_oncall(_ss)
    if len(titles) < 2:
        return 0.0

    total_unh = total_mc = total_on = 0.0

    # 1) UNH + MC
    for idx, label in enumerate(("UNH", "MC")):
        if len(titles) <= idx:
            continue
        t = titles[idx]
        try:
            ws = _ss.worksheet(t)
            if _hours_debug_enabled():
                # slow/raw grid path (useful for one-off diagnostics)
                subtotal = _count_half_hour_grid(ws, canon_name)
            else:
                # fast path via schedule day maps
                try:
                    info = _schedule._get_sheet(t)
                    subtotal = 0.0
                    for col in info.header_map.values():
                        day_map = read_day_column_map_cached(info, col, ttl_sec=DAY_CACHE_TTL_SEC)
                        for v in day_map.values():
                            if _cell_mentions_person(str(v), canon_name):
                                subtotal += 0.5
                except Exception:
                    subtotal = _count_half_hour_grid(ws, canon_name)
        except Exception:
            subtotal = 0.0

        if label == "UNH":
            total_unh = subtotal
        else:
            total_mc = subtotal

    # 2) On-Call (neighbor to MC)
    if len(titles) >= 3:
        try:
            ws_on = _ss.worksheet(titles[2])
            total_on = _count_oncall_by_day_headers(ws_on, canon_name)
        except Exception:
            total_on = 0.0

    total = total_unh + total_mc + total_on
    # Correct cap to a maximum of 20 hours.
    return total


def invalidate_hours_caches():
    st.session_state["HOURS_EPOCH"] = st.session_state.get("HOURS_EPOCH", 0) + 1


def total_hours_from_unh_mc_and_neighbor(_ss: gspread.Spreadsheet, _schedule, canon_name: str) -> float:
    """
    Fresh (non-cached) strict total used for the 20h cap check when adding shifts.
    Returns the **uncapped** total.
    """
    titles = _three_titles_unh_mc_oncall(_ss)
    _clear_ws_cache_for_titles(_ss, _schedule, titles)

    total_unh = total_mc = total_on = 0.0

    # UNH + MC (fresh)
    for idx, label in enumerate(("UNH", "MC")):
        if len(titles) <= idx:
            continue
        t = titles[idx]
        try:
            ws = _ss.worksheet(t)
            # In fresh mode, prefer raw grid
            subtotal = _count_half_hour_grid(ws, canon_name)
        except Exception:
            subtotal = 0.0
        if label == "UNH":
            total_unh = subtotal
        else:
            total_mc = subtotal

    # Neighbor (On-Call)
    if len(titles) >= 3:
        try:
            ws_on = _ss.worksheet(titles[2])
            total_on = _count_oncall_by_day_headers(ws_on, canon_name)
        except Exception:
            total_on = 0.0

    return total_unh + total_mc + total_on
