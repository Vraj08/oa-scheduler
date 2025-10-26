import re
from datetime import datetime, date, time, timedelta
from dateutil import parser as dateparser
from .config import DAY_START, DAY_END
import re, unicodedata

def collapse_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()

def matches_exact_oa_label(cell_value: str, canon_roster_name: str) -> bool:
    """
    True only if the cell is exactly 'OA: <Roster Name>' (case-insensitive on 'OA', 
    tolerant of spacing), and the name equals the exact roster display name.
    """
    if cell_value is None:
        return False
    s = unicodedata.normalize("NFKC", str(cell_value)).strip()
    # Must start with OA: (case-insensitive), then a name
    m = re.match(r"^\s*oa:\s*(.+?)\s*$", s, flags=re.I)
    if not m:
        return False
    cell_name = collapse_spaces(m.group(1))
    # Compare the remainder to the exact roster display name (space-normalized, case-sensitive by default)
    # If your sheet’s names may vary in capitalization, switch to case-insensitive compare by `.lower()`
    return cell_name == collapse_spaces(canon_roster_name)

def canon_name_like_cell(s: str) -> str:
    """
    Normalize a cell's value for reliable name comparison:
    - Unicode normalize (NFKC)
    - strip OA:/GOA: prefixes
    - lowercase
    - collapse all whitespace
    """
    if s is None:
        return ""
    # unicode normalize
    s = unicodedata.normalize("NFKC", str(s))
    # remove prefixes
    s = re.sub(r"^\s*(?:OA:|GOA:)\s*", "", s, flags=re.I)
    # collapse whitespace
    s = re.sub(r"\s+", " ", s).strip()
    # lowercase
    return s.lower()
import re, unicodedata

def canon_name_like_cell(s: str) -> str:
    """
    Normalize a cell's value for reliable name comparison:
    - Unicode normalize (NFKC)
    - strip OA:/GOA: prefixes
    - lowercase
    - collapse all whitespace
    """
    if s is None:
        return ""
    # unicode normalize
    s = unicodedata.normalize("NFKC", str(s))
    # remove prefixes
    s = re.sub(r"^\s*(?:OA:|GOA:)\s*", "", s, flags=re.I)
    # collapse whitespace
    s = re.sub(r"\s+", " ", s).strip()
    # lowercase
    return s.lower()

def normalize_day(day: str) -> str:
    d = (day or "").strip().lower()
    mapping = {
        "mon":"monday","monday":"monday",
        "tue":"tuesday","tues":"tuesday","tuesday":"tuesday",
        "wed":"wednesday","weds":"wednesday","wednesday":"wednesday",
        "thu":"thursday","thur":"thursday","thurs":"thursday","thursday":"thursday",
        "fri":"friday","friday":"friday",
        "sat":"saturday","saturday":"saturday",
        "sun":"sunday","sunday":"sunday",
    }
    if d in mapping: return mapping[d]
    raise ValueError(f"Unknown day: {day}")

def name_key(name: str) -> str:
    return re.sub(r"\s+", " ", (name or "")).strip().lower()

def normalize_campus(campus: str|None, default_campus: str) -> str:
    if not campus: return default_campus.split()[0]
    c = campus.strip().lower()
    if c.startswith("unh"): return "UNH"
    if c.startswith("mc") or "main" in c: return "MC"
    return campus.split()[0].upper()

def clean_dash(text: str) -> str:
    s = (text or "").replace("–", "-").replace("—", "-")
    s = re.sub(r"\b(to|till|til|until)\b", "-", s, flags=re.I)
    s = re.sub(r"\s*-\s*", "-", s)
    return s

def parse_time_str(t: str) -> time:
    t = (t or "").strip().lower().replace(".", "")
    t = re.sub(r"\b(\d{1,2}(?::\d{2})?)\s*([ap])\b", lambda m: f"{m.group(1)}{m.group(2)}m", t)
    if t in {"24:00","24","midnight","12am","12:00am"}: return time(0,0)
    try:
        dt = dateparser.parse(t)
        return time(dt.hour, dt.minute)
    except Exception:
        m = re.match(r"^(\d{1,2})(?::(\d{2}))?$", t)
        if m:
            hh = int(m.group(1)) % 24; mm = int(m.group(2) or 0)
            return time(hh, mm)
        raise ValueError(f"Could not parse time: {t}")

def has_ampm(s: str) -> str|None:
    m = re.search(r"(am|pm)\b", (s or "").strip(), flags=re.I)
    return m.group(1).lower() if m else None

def infer_range_am_pm(s_raw: str, e_raw: str, s: time, e: time) -> tuple[time,time]:
    s_mark, e_mark = has_ampm(s_raw), has_ampm(e_raw)
    if not s_mark and (e_mark == "pm" or e.hour >= 12):
        if 1 <= s.hour <= 11: s = time((s.hour % 12) + 12, s.minute)
    elif not s_mark and (e_mark == "am" or e.hour < 12):
        if s.hour == 12: s = time(0, s.minute)
    if not e_mark and s_mark in {"am","pm"}:
        if s_mark == "pm" and e.hour < 12: e = time((e.hour % 12) + 12, e.minute)
        elif s_mark == "am" and e.hour == 12: e = time(0, e.minute)
    return s, e

def assert_30min_grid(t: time) -> None:
    if t.minute % 30 != 0:
        raise ValueError("Time must be on a 30-minute boundary (e.g., 11:00, 11:30).")

def within_guardrails(t: time) -> None:
    if t < DAY_START or t > DAY_END:
        raise ValueError("Shifts must be between 7:00 AM and 12:00 AM (midnight).")

def time_slots(start: time, end: time) -> list[time]:
    if start == end: raise ValueError("End time must be after start time.")
    assert_30min_grid(start); assert_30min_grid(end)
    within_guardrails(start)
    if end != time(0,0): within_guardrails(end)
    today = date.today()
    cur = datetime.combine(today, start)
    stop = datetime.combine(today, time(23,59)) + timedelta(minutes=1) if end == time(0,0) else datetime.combine(today, end)
    if stop <= cur: raise ValueError("End time must be after start time.")
    out = []
    while cur < stop:
        out.append(cur.time()); cur += timedelta(minutes=30)
    return out

def fmt_time(t: time) -> str:
    return t.strftime("%I:%M %p").lstrip("0")

def is_time_token(x: str) -> time|None:
    if x is None: return None
    s = str(x).strip()
    if not s or s.lower() == "time": return None
    try: return parse_time_str(s)
    except Exception: return None
