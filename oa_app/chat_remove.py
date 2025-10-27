# oa_app/chat_remove.py
from __future__ import annotations
from datetime import datetime, timedelta
from typing import List, Optional
import gspread
import streamlit as st

from .utils import fmt_time
from .locks import get_or_create_locks_sheet, acquire_fcfs_lock, lock_key
from .hours import total_hours_from_unh_mc_and_neighbor
from .schedule_query import (
    _read_grid,
    _RANGE_RE,
    _TIME_CELL_RE,
    _parse_time_cell,
)
from .chat_add import (
    _ensure_dt, _is_half_hour_boundary_dt, _range_to_slots,
    _canon_input_day,
    _day_cols_from_first_row, _header_day_cols,
    _find_day_col_anywhere, _find_day_col_fuzzy,
    _infer_day_cols_by_blocks, _find_oncall_block_row_bounds,
    _find_working_oncall_ws, _resolve_campus_title, _is_blankish
)


def handle_remove(
    st, ss, schedule, *,
    canon_target_name: str,
    campus_title: str,
    day: str,
    start, end
) -> str:
    """
    Removes OA from the specified shift/block (UNH / MC / On-Call).

    • Handles overnight times (e.g., 7 PM – 12 AM, 10 PM – 2 AM)
    • Prints detailed debug if anything fails
    • Keeps logic identical to add()
    """
    debug_log: List[str] = []

    def dbg(msg: str):
        debug_log.append(str(msg))

    def fail(msg: str):
        log = "\n".join(debug_log[-800:])
        raise ValueError(f"{msg}\n\n--- DEBUG (remove) -------------------------\n{log if log else '(no debug)'}\n-------------------------------------------")

    # ─────────── Resolve tab/kind ───────────
    sidebar_tab = st.session_state.get("active_sheet")
    sheet_title, campus_kind = _resolve_campus_title(ss, campus_title, sidebar_tab)
    dbg(f"📌 Using sheet: '{sheet_title}' ({campus_kind})")

    # ─────────── Normalize times ───────────
    start_dt = _ensure_dt(start)
    end_dt   = _ensure_dt(end, ref_date=start_dt.date())

    if not (_is_half_hour_boundary_dt(start_dt) and _is_half_hour_boundary_dt(end_dt)):
        fail("Times must be on 30-minute boundaries (:00 or :30).")

    if end_dt <= start_dt:
        # handle overnight windows
        if 0 <= end_dt.time().hour <= 5:
            end_dt = end_dt + timedelta(days=1)
            dbg("⏩ Interpreting after-midnight end time as same-night; rolled end to next day.")
        else:
            fail("End time must be after start time.")

    req_slots = _range_to_slots(start_dt, end_dt)
    dbg(f"🕒 Removing {fmt_time(start_dt)}–{fmt_time(end_dt)} ({len(req_slots)*0.5:.1f}h)")

    # ─────────── Canonical day ───────────
    day_canon = _canon_input_day(day)
    if not day_canon:
        fail(f"Couldn't understand the day '{day}'.")
    dbg(f"🧭 Day raw={repr(day)} canon={day_canon!r}")

    # ─────────── ON-CALL ───────────
    if campus_kind == "ONCALL":
        try:
            ws0: gspread.Worksheet = ss.worksheet(sheet_title)
        except Exception as e:
            fail(f"Could not open worksheet '{sheet_title}': {e}")

        # Lock for concurrency
        locks_ws = get_or_create_locks_sheet(ss)
        k = lock_key(ws0.title, day_canon, fmt_time(start_dt), fmt_time(end_dt))
        won, _ = acquire_fcfs_lock(locks_ws, k, canon_target_name, ttl_sec=90)
        if not won:
            fail("Another request just claimed this window. Try again.")

        grid = _read_grid(ws0)
        if not grid:
            fail("On-Call sheet empty.")

        # Determine column
        day_cols = _day_cols_from_first_row(grid, dbg=dbg)
        if day_canon not in day_cols:
            hdr = _header_day_cols(grid, dbg=dbg)
            for k, v in hdr.items():
                day_cols.setdefault(k, v)
        if day_canon not in day_cols:
            inferred = _infer_day_cols_by_blocks(grid, dbg=dbg)
            for k, v in inferred.items():
                day_cols.setdefault(k, v)
        if day_canon not in day_cols:
            c_guess = _find_day_col_anywhere(grid, day_canon)
            if c_guess is not None:
                day_cols[day_canon] = c_guess
        if day_canon not in day_cols:
            c_guess2 = _find_day_col_fuzzy(grid, day_canon, dbg=dbg)
            if c_guess2 is not None:
                day_cols[day_canon] = c_guess2
        if day_canon not in day_cols:
            fail(f"Could not read weekday header from '{ws0.title}'.")

        c0 = day_cols[day_canon]
        c1 = c0  # On-Call uses same column for name + time range
        dbg(f"✅ Day '{day_canon.title()}' resolved to column {c0}")

        want_s = start_dt.strftime("%I:%M %p")
        want_e = end_dt.strftime("%I:%M %p")
        bounds = _find_oncall_block_row_bounds(grid, c0, want_s, want_e)
        if not bounds:
            fail(f"Could not locate On-Call block '{want_s} – {want_e}' in '{ws0.title}'.")
        r_label, r_next = bounds
        lane_rows = list(range(r_label + 1, r_next))
        if not lane_rows:
            fail("On-Call block has no lane rows defined.")

        cleared_any = False
        for rr in lane_rows:
            v = grid[rr][c0] if (rr < len(grid) and c0 < len(grid[rr])) else ""
            if v and canon_target_name.lower() in str(v).lower():
                ws0.update_cell(rr + 1, c1 + 1, "")  # gspread is 1-based
                dbg(f"🧹 Cleared r{rr+1}, c{c1+1} (was: {v!r})")
                cleared_any = True
        if not cleared_any:
            fail(f"{canon_target_name} not found in block '{want_s} – {want_e}'.")

        target_title = ws0.title

    # ─────────── UNH / MC ───────────
    else:
        info = schedule._get_sheet(sheet_title)
        ws0: gspread.Worksheet = getattr(info, 'ws', info)
        grid = _read_grid(ws0)
        if not grid:
            fail("Roster sheet empty.")

        # Determine weekday column
        day_cols = _header_day_cols(grid, dbg=dbg)
        if day_canon not in day_cols:
            c_guess = _find_day_col_anywhere(grid, day_canon)
            if c_guess is not None:
                day_cols[day_canon] = c_guess
        if day_canon not in day_cols:
            fail(f"Could not read weekday header (day '{day_canon}' missing).")

        c0 = day_cols[day_canon]; c1 = c0 + 1

        # Build 30-min slot bands from first column times
        bands = {}
        rows = []
        for r, row in enumerate(grid):
            ctime = (row[0] if len(row) >= 1 else "") or ""
            if _TIME_CELL_RE.match(ctime) and _parse_time_cell(ctime):
                rows.append(r)
        rows.append(len(grid))
        for i in range(len(rows) - 1):
            r0, r1 = rows[i], rows[i + 1]
            dt = _parse_time_cell(grid[r0][0])
            if dt:
                label = dt.strftime("%I:%M %p").lstrip("0")
                bands[label] = (r0, r1)

        for (sdt, _edt) in _range_to_slots(start_dt, end_dt):
            label = sdt.strftime("%I:%M %p").lstrip("0")
            if label not in bands:
                fail(f"Slot {label} not found in sheet.")
            r0, r1 = bands[label]
            lane_rows = list(range(r0 + 1, r1))
            cleared = False
            for rr in lane_rows:
                v = grid[rr][c0] if (rr < len(grid) and c0 < len(grid[rr])) else ""
                if v and canon_target_name.lower() in str(v).lower():
                    ws0.update_cell(rr + 1, c1 + 1, "")
                    dbg(f"🧹 Cleared {label} at r{rr+1}, c{c1+1} (was: {v!r})")
                    cleared = True
            if not cleared:
                dbg(f"⚠️ {label}: {canon_target_name} not found in any lane.")

        target_title = ws0.title

    # ─────────── Wrap up ───────────
    fresh_total = total_hours_from_unh_mc_and_neighbor(ss, schedule, canon_target_name)
    return (
        f"Removed **{canon_target_name}** from **{target_title}** — "
        f"{day_canon.title()} {fmt_time(start_dt)}–{fmt_time(end_dt)}. "
        f"Now at **{fresh_total:.1f}h / 20h** this week."
    )
