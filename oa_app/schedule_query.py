# oa_app/schedule_query.py
import time
import re
from datetime import datetime, timedelta, date
from typing import Dict, List, Tuple, Optional, Set
import streamlit as st
import gspread
import gspread.utils as a1
import pandas as pd
import plotly.express as px

from .config import (
    OA_SCHEDULE_SHEETS,   # e.g. ["UNH (OA and GOAs)", "MC (OA and GOAs)"]
    AUDIT_SHEET,
    LOCKS_SHEET,
    ONCALL_MAX_COLS,
    ONCALL_MAX_ROWS,
)
# Optional override; if missing, treat as None
try:
    from .config import ONCALL_SHEET_OVERRIDE as _ONCALL_OVERRIDE
except Exception:
    _ONCALL_OVERRIDE = None

from .quotas import _safe_batch_get

# ──────────────────────────────────────────────────────────────────────────────
# 2) Normalize the OA name (case-insensitive substring matching)
# ──────────────────────────────────────────────────────────────────────────────

def _norm_name(s: str) -> str:
    return (s or "").strip().lower()

def _cell_has_name(cell: str, name_norm: str) -> bool:
    if not cell or not name_norm:
        return False
    return name_norm in str(cell).lower()

# ──────────────────────────────────────────────────────────────────────────────
# Day helpers
# ──────────────────────────────────────────────────────────────────────────────

_DAY_WORDS = {
    "monday": "monday", "mon": "monday",
    "tuesday": "tuesday", "tue": "tuesday", "tues": "tuesday",
    "wednesday": "wednesday", "wed": "wednesday",
    "thursday": "thursday", "thu": "thursday", "thur": "thursday", "thurs": "thursday",
    "friday": "friday", "fri": "friday",
    "saturday": "saturday", "sat": "saturday",
    "sunday": "sunday", "sun": "sunday",
}
_WEEK_ORDER_7 = ["sunday", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday"]

def _canon_day_from_header(value: str) -> Optional[str]:
    """
    Row0 may contain "Monday".."Friday" (UNH/MC) or "Monday, 9/8/25" (On-Call).
    Return canonical day string or None.
    """
    s = (value or "").strip().lower()
    # keep letters, commas, spaces
    s = "".join(ch for ch in s if ch.isalpha() or ch.isspace() or ch == ",")
    head = s.split(",")[0].strip()
    return _DAY_WORDS.get(head)

# ──────────────────────────────────────────────────────────────────────────────
# Time parsing + formatting
# ──────────────────────────────────────────────────────────────────────────────

_TIME_CELL_RE = re.compile(r"^\s*\d{1,2}:\d{2}\s*(?:AM|PM)\s*$", re.I)
_RANGE_RE = re.compile(
    r"^\s*(\d{1,2}:\d{2}\s*(?:AM|PM))\s*[-–]\s*(\d{1,2}:\d{2}\s*(?:AM|PM))\s*$",
    re.I,
)

def _parse_time_cell(s: str) -> Optional[datetime]:
    try:
        return datetime.strptime(s.strip(), "%I:%M %p")
    except Exception:
        return None

def _fmt(dt: datetime) -> str:
    try:
        return dt.strftime("%-I:%M %p")  # POSIX
    except Exception:
        return dt.strftime("%I:%M %p")   # Windows

# ──────────────────────────────────────────────────────────────────────────────
# Robust worksheet resolution (handles disconnects)
# ──────────────────────────────────────────────────────────────────────────────

def _resolve_title(actuals: List[gspread.Worksheet], wanted: str) -> Optional[str]:
    want = (wanted or "").strip().lower()
    by_low = {w.title.strip().lower(): w.title for w in actuals}
    if want in by_low:
        return by_low[want]
    first = want.split()[0] if want else ""
    for w in actuals:
        t = w.title.strip()
        tl = t.lower()
        if tl == want or (first and tl.startswith(first)):
            return t
    return None

def _list_worksheets_with_retry(ss: gspread.Spreadsheet, attempts: int = 4, base_sleep: float = 0.4) -> Optional[List[gspread.Worksheet]]:
    """Retry listing worksheets to survive transient RemoteDisconnected."""
    for i in range(attempts):
        try:
            return ss.worksheets()
        except Exception:
            if i == attempts - 1:
                return None
            time.sleep(base_sleep * (2 ** i))
    return None
@st.cache_data(ttl=60, show_spinner=False)
def _cached_ws_titles(ss_id: str) -> list[str]:
    """
    Cached list of *visible* worksheet titles for this Spreadsheet id.
    Uses the Spreadsheet handle we stashed in session_state (set in app.py).
    """
    ss = st.session_state.get("_SS_HANDLE_BY_ID", {}).get(ss_id)
    if ss is None:
        return []
    try:
        lst = ss.worksheets()
    except Exception:
        return []
    titles = []
    for w in lst:
        hidden = bool(getattr(w, "_properties", {}).get("hidden", False))
        if not hidden:
            titles.append(w.title)
    return titles

def _open_three(ss: gspread.Spreadsheet) -> List[str]:
    """
    Return [UNH, MC, On-Call?] using a cached list of titles for 60s
    to avoid hammering ss.worksheets(). Falls back to a retry listing if cache empty.
    """
    unh_cfg, mc_cfg = OA_SCHEDULE_SHEETS[0], OA_SCHEDULE_SHEETS[1]
    out: List[str] = []

    # 1) Try cached titles first
    titles = _cached_ws_titles(getattr(ss, "id", ""))
    def _resolve_from_titles(all_titles: list[str], wanted: str) -> Optional[str]:
        want = (wanted or "").strip().lower()
        by_low = {t.strip().lower(): t for t in all_titles}
        if want in by_low:
            return by_low[want]
        first = want.split()[0] if want else ""
        for t in all_titles:
            tl = t.strip().lower()
            if tl == want or (first and tl.startswith(first)):
                return t
        return None

    if titles:
        unh = _resolve_from_titles(titles, unh_cfg)
        mc  = _resolve_from_titles(titles, mc_cfg)
        if unh: out.append(unh)
        if mc:  out.append(mc)

        # On-Call (override → else neighbor to MC)
        oncall = None
        if mc:
            if _ONCALL_OVERRIDE and str(_ONCALL_OVERRIDE).strip():
                cand = _resolve_from_titles(titles, str(_ONCALL_OVERRIDE))
                if cand: oncall = cand
            if not oncall:
                try:
                    idx = titles.index(mc)
                except ValueError:
                    idx = -1
                if idx >= 0:
                    deny = {str(AUDIT_SHEET).strip().lower(), str(LOCKS_SHEET).strip().lower()}
                    j = idx + 1
                    while j < len(titles):
                        cand = titles[j]
                        if cand.strip().lower() not in deny:
                            oncall = cand
                            break
                        j += 1
        if oncall:
            out.append(oncall)

        # de-dup preserving order
        seen, final = set(), []
        for t in out:
            if t and t not in seen:
                seen.add(t); final.append(t)
        return final

    # 2) Fallback (rare): cached titles unavailable → old retry path
    ws_list = _list_worksheets_with_retry(ss)
    out = []
    if ws_list is not None:
        unh = _resolve_title(ws_list, unh_cfg)
        mc  = _resolve_title(ws_list, mc_cfg)
        if unh: out.append(unh)
        if mc:  out.append(mc)

        oncall = None
        if mc:
            if _ONCALL_OVERRIDE and str(_ONCALL_OVERRIDE).strip():
                oncall = _resolve_title(ws_list, str(_ONCALL_OVERRIDE))
            if not oncall:
                deny = {str(AUDIT_SHEET).strip().lower(), str(LOCKS_SHEET).strip().lower()}
                try:
                    idx = next(i for i, w in enumerate(ws_list) if w.title == mc)
                except StopIteration:
                    idx = -1
                if idx >= 0:
                    j = idx + 1
                    while j < len(ws_list):
                        cand = ws_list[j].title
                        if cand.strip().lower() not in deny:
                            oncall = cand
                            break
                        j += 1
        if oncall:
            out.append(oncall)

    # de-dup
    seen, final = set(), []
    for t in out:
        if t and t not in seen:
            seen.add(t); final.append(t)
    return final

# ──────────────────────────────────────────────────────────────────────────────
# 3) UNH/MC: 30-minute slots → merged ranges (exact algorithm requested)
# ──────────────────────────────────────────────────────────────────────────────

def _read_grid(ws: gspread.Worksheet) -> List[List[str]]:
    end_col_letter = a1.rowcol_to_a1(1, ONCALL_MAX_COLS).split("1")[0]
    return _safe_batch_get(ws, [f"A1:{end_col_letter}{ONCALL_MAX_ROWS}"])[0] or []

def _unh_mc_intervals(ws: gspread.Worksheet, name_norm: str) -> Dict[str, List[Tuple[datetime, datetime]]]:
    grid = _read_grid(ws)
    if not grid:
        return {}

    # 3.1 Identify structure
    header = grid[0] if grid else []
    day_cols: Dict[str, int] = {}
    for c, val in enumerate(header):
        d = _canon_day_from_header(val)
        if d and d not in day_cols:
            day_cols[d] = c

    # Col 0 contains time row labels; find their row indices
    time_rows: List[int] = []
    for r, row in enumerate(grid):
        col0 = (row[0] if len(row) >= 1 else "") or ""
        if _TIME_CELL_RE.match(col0) and _parse_time_cell(col0):
            time_rows.append(r)

    if not time_rows:
        return {}

    # Sentinel at bottom
    time_rows.append(len(grid))

    # 3.2 Build “hit” intervals
    hits: Dict[str, List[Tuple[datetime, datetime]]] = {d: [] for d in day_cols}
    for i in range(len(time_rows) - 1):
        r0 = time_rows[i]
        r1 = time_rows[i + 1]

        start_label = (grid[r0][0] if len(grid[r0]) >= 1 else "") or ""
        start_dt = _parse_time_cell(start_label)
        if not start_dt:
            continue
        end_dt = start_dt + timedelta(minutes=30)

        for day, c in day_cols.items():
            if c == 0:
                continue  # skip time column

            # Gather all text from the slot rows between this time row and the next
            parts: List[str] = []
            for rr in range(r0 + 1, r1):
                val = grid[rr][c] if len(grid[rr]) > c else ""
                if val:
                    parts.append(str(val))
            blob = " ".join(parts).strip().lower()
            if blob and _cell_has_name(blob, name_norm):
                hits[day].append((start_dt, end_dt))

    return hits

def _merge_contiguous(intervals: List[Tuple[datetime, datetime]]) -> List[Tuple[datetime, datetime]]:
    if not intervals:
        return []
    intervals.sort(key=lambda x: x[0])
    merged: List[Tuple[datetime, datetime]] = []
    cur_s, cur_e = intervals[0]
    for s, e in intervals[1:]:
        if cur_e == s:        # contiguous half-hours
            cur_e = e
        elif e <= cur_e:      # overlaps/dups collapse
            continue
        else:
            merged.append((cur_s, cur_e))
            cur_s, cur_e = s, e
    merged.append((cur_s, cur_e))
    return merged

def _unh_mc_ranges(ws: gspread.Worksheet, name_norm: str) -> Dict[str, List[Tuple[str, str]]]:
    intervals_by_day = _unh_mc_intervals(ws, name_norm)
    out: Dict[str, List[Tuple[str, str]]] = {}
    for day, ivals in intervals_by_day.items():
        merged = _merge_contiguous(ivals)
        if merged:
            out[day] = [(_fmt(s), _fmt(e)) for s, e in merged]
    return out

# ──────────────────────────────────────────────────────────────────────────────
# 4) On-Call: 4/5-hour blocks (time range label followed by names)
# ──────────────────────────────────────────────────────────────────────────────

def _oncall_blocks(ws: gspread.Worksheet, name_norm: str) -> Dict[str, List[Tuple[str, str]]]:
    grid = _read_grid(ws)
    if not grid:
        return {}

    # 4.1 Identify structure (day columns from row 0)
    day_cols: Dict[str, int] = {}
    for c, top in enumerate(grid[0]):
        d = _canon_day_from_header(top)
        if d and d not in day_cols:
            day_cols[d] = c

    per_day: Dict[str, Set[Tuple[str, str]]] = {d: set() for d in day_cols}

    # 4.2 Extract blocks per day
    for day, c in day_cols.items():
        current_range: Optional[Tuple[str, str]] = None
        for r in range(1, len(grid)):
            cell = (grid[r][c] if len(grid[r]) > c else "") or ""
            m = _RANGE_RE.match(cell) if cell else None
            if m:
                s_raw, e_raw = m.group(1), m.group(2)
                sdt, edt = _parse_time_cell(s_raw), _parse_time_cell(e_raw)
                if sdt and edt:
                    current_range = (_fmt(sdt), _fmt(edt))
                else:
                    current_range = None
            else:
                if current_range and _cell_has_name(cell, name_norm):
                    per_day[day].add(current_range)

    # Dedup + sort by start time
    out: Dict[str, List[Tuple[str, str]]] = {}
    for d, blocks in per_day.items():
        sorted_blocks = sorted(blocks, key=lambda ab: _parse_time_cell(ab[0]) or datetime.min)
        if sorted_blocks:
            out[d] = list(sorted_blocks)
    return out

# ──────────────────────────────────────────────────────────────────────────────
# Public API used by app.py
# ──────────────────────────────────────────────────────────────────────────────

def get_user_schedule(ss: gspread.Spreadsheet, _schedule_unused, oa_name: str) -> Dict[str, Dict[str, List[Tuple[str, str]]]]:
    """
    Returns:
      {
        'monday': {'UNH': [(start,end),...], 'MC': [...], 'On-Call': [('start','end'), ...]},
        ...
      }
    """
    titles = _open_three(ss)
    result: Dict[str, Dict[str, List[Tuple[str, str]]]] = {
        d: {"UNH": [], "MC": [], "On-Call": []} for d in _WEEK_ORDER_7
    }

    if not titles:
        return result

    name_norm = _norm_name(oa_name)

    # UNH
    if len(titles) >= 1:
        try:
            ws_unh = ss.worksheet(titles[0])
            unh_ranges = _unh_mc_ranges(ws_unh, name_norm)  # Mon–Fri
            for d, blocks in unh_ranges.items():
                result[d]["UNH"] = blocks
        except Exception:
            pass

    # MC
    if len(titles) >= 2:
        try:
            ws_mc = ss.worksheet(titles[1])
            mc_ranges = _unh_mc_ranges(ws_mc, name_norm)  # Mon–Fri
            for d, blocks in mc_ranges.items():
                result[d]["MC"] = blocks
        except Exception:
            pass

    # On-Call (neighbor / override)
    if len(titles) >= 3:
        try:
            ws_on = ss.worksheet(titles[2])
            oc = _oncall_blocks(ws_on, name_norm)  # Sun–Sat
            for d, blocks in oc.items():
                result[d]["On-Call"].extend(blocks)
        except Exception:
            pass

    return result

# ──────────────────────────────────────────────────────────────────────────────
# Minimal, polished tabular rendering (weekly + per-day tables) — for chat
# ──────────────────────────────────────────────────────────────────────────────

def _mins_between(s: str, e: str) -> int:
    try:
        sd = datetime.strptime(s, "%I:%M %p")
        ed = datetime.strptime(e, "%I:%M %p")
    except Exception:
        return 0
    if ed <= sd:  # allow on-call to roll past midnight
        ed += timedelta(days=1)
    return int((ed - sd).total_seconds() // 60)

def _sum_ranges_minutes(ranges: List[Tuple[str, str]]) -> int:
    return sum(_mins_between(s, e) for s, e in ranges)

def _fmt_hours(mins: int) -> str:
    h, m = mins // 60, mins % 60
    return f"{h}h" if m == 0 else f"{h}h {m}m"

def _join_chips(ranges: List[Tuple[str, str]]) -> str:
    # compact “chips” for time blocks
    return ", ".join(f"`{s} – {e}`" for s, e in ranges)

def _weekly_totals(user_sched: Dict[str, Dict[str, List[Tuple[str, str]]]]) -> Dict[str, int]:
    totals = {"UNH": 0, "MC": 0, "On-Call": 0}
    for buckets in user_sched.values():
        totals["UNH"]    += _sum_ranges_minutes(buckets.get("UNH", []))
        totals["MC"]     += _sum_ranges_minutes(buckets.get("MC", []))
        totals["On-Call"]+= _sum_ranges_minutes(buckets.get("On-Call", []))
    return totals

def _render_weekly_summary_table(user_sched: Dict[str, Dict[str, List[Tuple[str, str]]]]) -> List[str]:
    totals = _weekly_totals(user_sched)
    lines = []
    lines.append("## Weekly Summary")
    lines.append("| Source | Hours |")
    lines.append("|:------:|:-----:|")
    lines.append(f"| UNH | **{_fmt_hours(totals['UNH'])}** |")
    lines.append(f"| MC | **{_fmt_hours(totals['MC'])}** |")
    lines.append(f"| On-Call | **{_fmt_hours(totals['On-Call'])}** |")
    return lines

def _render_day_table(day: str, buckets: Dict[str, List[Tuple[str, str]]]) -> List[str]:
    # Only include rows for sources that have blocks
    rows = []
    if buckets.get("UNH"):
        mins = _sum_ranges_minutes(buckets["UNH"])
        rows.append(("UNH", _join_chips(buckets["UNH"]), _fmt_hours(mins)))
    if buckets.get("MC"):
        mins = _sum_ranges_minutes(buckets["MC"])
        rows.append(("MC", _join_chips(buckets["MC"]), _fmt_hours(mins)))
    if buckets.get("On-Call"):
        mins = _sum_ranges_minutes(buckets["On-Call"])
        rows.append(("On-Call", _join_chips(buckets["On-Call"]), _fmt_hours(mins)))

    if not rows:
        return []

    lines = []
    lines.append(f"### {day.title()}")
    lines.append("| Source | Time Blocks | Total |")
    lines.append("|:------:|:-----------|:-----:|")
    for src, chips, total in rows:
        lines.append(f"| {src} | {chips} | **{total}** |")
    return lines

def render_user_schedule_markdown(
    user_sched: Dict[str, Dict[str, List[Tuple[str, str]]]],
    *,
    include_weekly_summary: bool = True
) -> str:
    """
    Clean tabular layout for chat:
      • Weekly Summary table (optional)
      • One compact table per day that has shifts
    """
    day_order = ["monday","tuesday","wednesday","thursday","friday","saturday","sunday"]
    blocks: List[str] = []

    if include_weekly_summary:
        blocks.extend(_render_weekly_summary_table(user_sched))
        blocks.append("")  # spacer

    any_day = False
    for d in day_order:
        section = _render_day_table(d, user_sched.get(d, {}))
        if section:
            any_day = True
            blocks.extend(section)
            blocks.append("")  # spacer between days

    if not any_day:
        return "_No shifts found for your name._"

    # trim last blank line
    if blocks and blocks[-1] == "":
        blocks.pop()

    return "\n".join(blocks)

def chat_schedule_response(ss, schedule_unused, oa_name: str) -> str:
    data = get_user_schedule(ss, schedule_unused, oa_name)
    # set include_weekly_summary=False if you prefer only per-day tables
    return render_user_schedule_markdown(data, include_weekly_summary=True)

# ──────────────────────────────────────────────────────────────────────────────
# PICTORIAL / TABULAR RENDERING (MAIN APP PANE)
# ──────────────────────────────────────────────────────────────────────────────

# Brand-ish colors for sources (tweak to taste)
_SOURCE_COLOR = {
    "UNH": "#4F46E5",      # indigo-600
    "MC": "#16A34A",       # green-600
    "On-Call": "#F59E0B",  # amber-500
}

_DAY_ORDER = ["monday","tuesday","wednesday","thursday","friday","saturday","sunday"]
_DAY_TITLE = {d: d.title() for d in _DAY_ORDER}

def _parse_time_for_dt(t: str) -> datetime:
    return datetime.strptime(t.strip(), "%I:%M %p")

def _anchor_dt(t: str, anchor: date) -> datetime:
    base = _parse_time_for_dt(t)
    return datetime(anchor.year, anchor.month, anchor.day, base.hour, base.minute, base.second)

def build_schedule_dataframe(user_sched: Dict[str, Dict[str, List[Tuple[str, str]]]]) -> pd.DataFrame:
    """
    Flatten parsed schedule into a DataFrame:
    Columns: Day, Source, Start, End, Date, DurationMin, Duration
    Plot-only: PlotStartDT, PlotEndDT
    """
    rows = []
    today = date.today()
    anchor_mon = today - timedelta(days=(today.weekday() % 7))
    anchors = {
        "monday":    anchor_mon,
        "tuesday":   anchor_mon + timedelta(days=1),
        "wednesday": anchor_mon + timedelta(days=2),
        "thursday":  anchor_mon + timedelta(days=3),
        "friday":    anchor_mon + timedelta(days=4),
        "saturday":  anchor_mon + timedelta(days=5),
        "sunday":    anchor_mon + timedelta(days=6),
    }

    for day in _DAY_ORDER:
        buckets = user_sched.get(day, {})
        for src in ("UNH","MC","On-Call"):
            for (s, e) in buckets.get(src, []) or []:
                plot_start = _anchor_dt(s, anchors[day])
                plot_end   = _anchor_dt(e, anchors[day])
                if plot_end <= plot_start:
                    plot_end += timedelta(days=1)

                dur_min = int((plot_end - plot_start).total_seconds() // 60)

                rows.append({
                    "Day": _DAY_TITLE[day],
                    "Source": src,
                    "Start": s,
                    "End": e,
                    "Date": plot_start.date(),   # <-- only date, replaces StartDT/EndDT
                    "DurationMin": dur_min,
                    "Duration": (f"{dur_min//60}h" if dur_min % 60 == 0
                                 else f"{dur_min//60}h {dur_min%60}m"),
                    "PlotStartDT": plot_start,  # keep full datetime for chart
                    "PlotEndDT": plot_end,
                })

    if not rows:
        return pd.DataFrame(columns=[
            "Day","Source","Start","End","Date","DurationMin","Duration",
            "PlotStartDT","PlotEndDT"
        ])

    df = pd.DataFrame(rows)
    df["DayOrder"] = df["Day"].map({v: i for i, v in enumerate([_DAY_TITLE[d] for d in _DAY_ORDER])})
    df.sort_values(["DayOrder","PlotStartDT","Source"], inplace=True, kind="stable")
    df.drop(columns=["DayOrder"], inplace=True)
    return df

def render_schedule_viz(st, df: pd.DataFrame, *, title: str = "This Week's Schedule"):
    """
    Calendar view:
      • X-axis: Days (Mon → Sun) with day+date labels shown at the top
      • Y-axis: Time (7:00 AM → 12:00 AM) in 30-min increments
      • Narrow colored blocks with centered labels: time range + source
      • Vertical separator lines between days
    """
    if df.empty:
        st.info("No shifts found for your name.")
        return

    try:
        import plotly.graph_objects as go
    except ImportError:
        st.info("📈 Install Plotly to enable the pictorial timeline: `pip install plotly`")
        return

    # ---- Days present (Mon→Sun order) ----
    day_titles = [_DAY_TITLE[d] for d in _DAY_ORDER]
    days_present = [d for d in day_titles if d in df["Day"].unique().tolist()]
    if not days_present:
        st.info("No shifts found for your name.")
        return

    # Map each day to a representative date
    day_to_date = (
        df.sort_values(["Day", "PlotStartDT"])
          .groupby("Day", as_index=False)
          .first()[["Day", "PlotStartDT"]]
    )
    day_to_date = {r["Day"]: r["PlotStartDT"].date() for _, r in day_to_date.iterrows()}

    def _fmt_day_with_date(day_name: str) -> str:
        d = day_to_date.get(day_name)
        return f"{day_name}<br>{d.strftime('%b %d')}" if d else day_name

    x_ticktext = [_fmt_day_with_date(d) for d in days_present]

    # ---- Helpers ----
    def _to_minutes(dt: datetime) -> int:
        m = dt.hour * 60 + dt.minute
        return 24 * 60 if m == 0 else m   # midnight → 1440

    def _label_time_range(start_dt: datetime, end_dt: datetime) -> str:
        """Return a clean time range label, e.g., '7–12 AM' or '11 AM–1 PM'."""
        def fmt(H: int, M: int):
            H12 = 12 if (H % 12) == 0 else (H % 12)
            return f"{H12}" if M == 0 else f"{H12}:{M:02d}"

        sH, sM = start_dt.hour, start_dt.minute
        eH, eM = end_dt.hour, end_dt.minute
        if eH == 0 and eM == 0:
            eH, eM = 24, 0  # treat midnight as 24:00

        s_ampm = "AM" if sH < 12 else "PM"
        e_ampm = "AM" if (eH % 24) < 12 else "PM"

        if s_ampm == e_ampm:
            return f"{fmt(sH, sM)}–{fmt(eH, eM)} {s_ampm}"
        else:
            return f"{fmt(sH, sM)} {s_ampm}–{fmt(eH, eM)} {e_ampm}"

    # ---- Build bar traces ----
    bars = []
    seen_source = set()
    for _, r in df.iterrows():
        day = r["Day"]
        if day not in days_present:
            continue

        start_min = max(_to_minutes(r["PlotStartDT"]), 7 * 60)
        end_min   = min(_to_minutes(r["PlotEndDT"]), 24 * 60)
        if end_min <= start_min:
            continue

        start_hr = start_min / 60.0
        dur_hr   = (end_min - start_min) / 60.0

        color = _SOURCE_COLOR.get(r["Source"], "#6b7280")
        showlegend = r["Source"] not in seen_source
        seen_source.add(r["Source"])

        label = f"{_label_time_range(r['PlotStartDT'], r['PlotEndDT'])}<br>{r['Source']}"

        bars.append(go.Bar(
            x=[day],
            y=[dur_hr],
            base=[start_hr],
            marker=dict(color=color, line=dict(width=0)),
            width=0.38,
            name=r["Source"],
            showlegend=showlegend,
            text=[label],
            texttemplate="%{text}",
            textposition="inside",
            insidetextanchor="middle",
            textfont=dict(color="white", size=11),
            hovertemplate=(f"{day} ({day_to_date.get(day,'')})<br>"
                           f"{r['Source']}<br>"
                           f"{r['Start']} – {r['End']}<extra></extra>"),
            opacity=0.98,
        ))

    fig = go.Figure(bars)

    # ---- Y axis ticks (30-min increments) ----
    y_ticks = [7 + i * 0.5 for i in range(int((24 - 7) / 0.5) + 1)]
    def _fmt_h(h):
        total_min = int(round(h * 60))
        H, M = divmod(total_min, 60)
        if H == 24: H = 0
        ampm = "AM" if H < 12 else "PM"
        H12 = 12 if H % 12 == 0 else H % 12
        return f"{H12}:{M:02d} {ampm}"

    y_text = [_fmt_h(h) for h in y_ticks]

    fig.update_xaxes(
        title="",
        type="category",
        tickmode="array",
        tickvals=days_present,
        ticktext=x_ticktext,
        showline=True,
        linecolor="#e5e7eb",
        ticks="outside",
        tickfont=dict(size=12),
        side="top",  # move labels to top
    )

    fig.update_yaxes(
        title="Time",
        tickmode="array",
        tickvals=y_ticks,
        ticktext=y_text,
        range=[24, 7],
        showgrid=True,
        gridcolor="#eef2f7",
        zeroline=False,
    )

    # ---- Vertical separator lines between days ----
    for i in range(1, len(days_present)):
        fig.add_vline(
            x=i - 0.5, line_width=1, line_dash="dot", line_color="#cccccc"
        )

    fig.update_layout(
        title=title,
        barmode="overlay",
        bargap=0.18,
        bargroupgap=0.05,
        plot_bgcolor="#ffffff",
        paper_bgcolor="#ffffff",
        legend_title_text="",
        margin=dict(l=10, r=10, t=60, b=10),
        height=max(820, int(26 * len(y_ticks))),
    )

    st.plotly_chart(fig, use_container_width=True, theme="streamlit")
def render_schedule_dataframe(st, df: pd.DataFrame):
    """
    Show a simple dataframe below the chart.
    Columns: Date, Day, Source, Start, End, Duration
    """
    if df.empty:
        st.info("No shifts found for your name.")
        return

    cols = ["Date", "Day", "Source", "Start", "End", "Duration"]
    show = df[cols].copy()

    # Sort nicely by date, then source, then start time
    show.sort_values(["Date", "Day", "Source"], inplace=True)

    st.markdown("### Full Schedule Table")
    st.dataframe(show, use_container_width=True)
