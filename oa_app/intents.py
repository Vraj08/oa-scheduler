import re
from dataclasses import dataclass
from datetime import datetime, date, timedelta
from datetime import time as dtime
from .utils import clean_dash, normalize_campus, parse_time_str, infer_range_am_pm

# ───────────────────────── Day handling ─────────────────────────
DAY_RE = r"(?P<day>Mon(?:day)?|Tue(?:s|sday)?|Wed(?:nesday)?|Thu(?:r|rs|rsday)?|Fri(?:day)?|Sat(?:urday)?|Sun(?:day)?)"

# Canonicalize user-provided weekday tokens like "wed", "wednes", "wednesd", etc.
# Returns one of: 'sunday'...'saturday', or None if unrecognized.
_USER_DAY_ALIASES = {
    "mon": "monday", "monday": "monday",
    "tue": "tuesday", "tues": "tuesday", "tuesday": "tuesday",
    "wed": "wednesday", "weds": "wednesday", "wednes": "wednesday", "wednesd": "wednesday", "wednesday": "wednesday",
    "thu": "thursday", "thur": "thursday", "thurs": "thursday", "thursday": "thursday",
    "fri": "friday", "friday": "friday",
    "sat": "saturday", "saturday": "saturday",
    "sun": "sunday", "sunday": "sunday",
}
_WEEKDAYS = ["sunday","monday","tuesday","wednesday","thursday","friday","saturday"]

def _canon_input_day(user_day: str | None) -> str | None:
    """Normalize loose user input like 'wed', 'wednes', 'wednesd' to a canonical weekday."""
    if not user_day:
        return None
    token = re.sub(r"[^a-z]", "", user_day.strip().lower())
    if not token:
        return None
    if token in _USER_DAY_ALIASES:
        return _USER_DAY_ALIASES[token]
    if len(token) >= 2:
        for full in _WEEKDAYS:
            if full.startswith(token):
                return full
    return None

def _extract_day_from_text(text: str) -> str | None:
    """Scan the whole sentence for any weekday token if a specific regex branch didn't capture it."""
    # 1) Try explicit DAY_RE anywhere
    m = re.search(DAY_RE, text, flags=re.IGNORECASE)
    if m:
        d = _canon_input_day(m.group("day"))
        if d: return d

    # 2) Try any weekday token by simple scan
    low = text.lower()
    # common abbrev tokens
    for tok in ["mon","monday","tue","tues","tuesday","wed","weds","wednes","wednesd","wednesday",
                "thu","thur","thurs","thursday","fri","friday","sat","saturday","sun","sunday"]:
        if re.search(rf"\b{tok}\b", low):
            d = _canon_input_day(tok)
            if d: return d
    return None

# ───────────────────────── Patterns ─────────────────────────
def time_re(name: str) -> str:
    return r"(?P<" + name + r">\d{1,2}(?::\d{2})?\s*(?:am|pm|[ap])?)"

RANGE_RE = time_re("s") + r"\s*(?:-|–|—|to|till|til|until)\s*" + time_re("e")
CAMPUS_RE = r"(UNH|MC|UNH \(OA(?: and GOAs)?\)|MC \(OA(?: and GOAs)?\)|Main Campus|Main|UNH Campus|Durham)"

ADD_RE = re.compile(r"\b(?P<verb>add|assign|book|schedule|put)\b.*?" + DAY_RE + r".*?" + RANGE_RE, re.I)
REM_RE = re.compile(r"\b(?P<verb>remove|delete|clear|cancel|drop|release)\b.*?" + DAY_RE + r".*?" + RANGE_RE, re.I)
CHG_RE = re.compile(
    r"\b(?P<verb>change|move|reschedule|update)\b.*?"
    + DAY_RE + r".*?from\s*" + time_re("s_old")
    + r"\s*(?:-|–|—|to|till|til|until)\s*" + time_re("e_old")
    + r"\s*to\s*" + time_re("s_new")
    + r"\s*(?:-|–|—|to|till|til|until)\s*" + time_re("e_new"),
    re.I,
)
SWP_RE = re.compile(r"\b(?P<verb>swap|trade)\b.*?" + DAY_RE + r".*?" + RANGE_RE + r".*?with\s+(?P<other>[A-Za-z\-.' ]+)", re.I)

# Looser add: "<day> 7-9" (no verb)
IMPLICIT_ADD_RE = re.compile(DAY_RE + r".*?" + RANGE_RE, re.I)

# Single-slot add (no explicit end): "add wed 1 pm", "book wed 7a", etc.
# Note: (?P<day>...) is OPTIONAL here → we must supply a robust fallback if it's missing.
SINGLE_ADD_RE = re.compile(
    r"\b(?P<verb>add|assign|book|schedule|put)\b.*?(?:(?P<day>Mon(?:day)?|Tue(?:s|sday)?|Wed(?:nesday)?|Thu(?:r|rs|rsday)?|Fri(?:day)?|Sat(?:urday)?|Sun(?:day)?))?.*?(?P<s>\d{1,2}(?::\d{2})?\s*(?:am|pm|[ap])?)",
    re.I,
)

# ───────────────────────── Model ─────────────────────────
@dataclass
class Intent:
    kind: str
    campus: str
    day: str
    start: dtime
    end: dtime
    name: str|None = None
    other_name: str|None = None
    old_start: dtime|None = None
    old_end: dtime|None = None

# ───────────────────────── Helpers ─────────────────────────
def _parse_and_infer(s_raw: str, e_raw: str) -> tuple[dtime,dtime]:
    s = parse_time_str(s_raw); e = parse_time_str(e_raw)
    return infer_range_am_pm(s_raw, e_raw, s, e)

# ───────────────────────── Parser ─────────────────────────
def parse_intent(text: str, default_campus: str, default_name: str) -> Intent:
    text = clean_dash(text)
    campus_match = re.search(CAMPUS_RE, text, flags=re.IGNORECASE)
    campus = normalize_campus(campus_match.group(1) if campus_match else None, default_campus)

    # 1) Full add: "add Wed 9-11"
    m = ADD_RE.search(text)
    if m:
        day = _canon_input_day(m.group("day")) or _extract_day_from_text(text) or datetime.today().strftime("%A").lower()
        s, e = _parse_and_infer(m.group("s"), m.group("e"))
        return Intent(kind="add", campus=campus, day=day, start=s, end=e, name=default_name)

    # 2) Remove: "remove Wed 9-11"
    m = REM_RE.search(text)
    if m:
        day = _canon_input_day(m.group("day")) or _extract_day_from_text(text) or datetime.today().strftime("%A").lower()
        s, e = _parse_and_infer(m.group("s"), m.group("e"))
        return Intent(kind="remove", campus=campus, day=day, start=s, end=e, name=default_name)

    # 3) Change: "change Wed from 9-11 to 11-1"
    m = CHG_RE.search(text)
    if m:
        day = _canon_input_day(m.group("day")) or _extract_day_from_text(text) or datetime.today().strftime("%A").lower()
        old_s, old_e = _parse_and_infer(m.group("s_old"), m.group("e_old"))
        new_s, new_e = _parse_and_infer(m.group("s_new"), m.group("e_new"))
        return Intent(kind="change", campus=campus, day=day, start=new_s, end=new_e, name=default_name,
                      old_start=old_s, old_end=old_e)

    # 4) Swap: "swap Wed 9-11 with Jane"
    m = SWP_RE.search(text)
    if m:
        day = _canon_input_day(m.group("day")) or _extract_day_from_text(text) or datetime.today().strftime("%A").lower()
        s, e = _parse_and_infer(m.group("s"), m.group("e"))
        other = m.group("other").strip()
        return Intent(kind="swap", campus=campus, day=day, start=s, end=e, name=default_name, other_name=other)

    # 5) Implicit add: "Wed 9-11"
    m = IMPLICIT_ADD_RE.search(text)
    if m:
        day = _canon_input_day(m.group("day")) or _extract_day_from_text(text) or datetime.today().strftime("%A").lower()
        s, e = _parse_and_infer(m.group("s"), m.group("e"))
        return Intent(kind="add", campus=campus, day=day, start=s, end=e, name=default_name)

    # 6) Single-slot add: "add wed 1 pm" (end is implied +30 min)
    m = SINGLE_ADD_RE.search(text)
    if m:
        # Day may be missing in this pattern → recover from text or default to today
        day = _canon_input_day(m.group("day")) or _extract_day_from_text(text) or datetime.today().strftime("%A").lower()
        s = parse_time_str(m.group("s"))
        if s.minute % 30 != 0:
            raise ValueError("Time must be on a 30-minute boundary (e.g., 7:00, 7:30).")
        e_dt = datetime.combine(date.today(), s) + timedelta(minutes=30)
        e = e_dt.time()
        return Intent(kind="add", campus=campus, day=day, start=s, end=e, name=default_name)

    raise ValueError("Sorry, I couldn't understand. Examples: 'add Wed 9-11', 'change Wed from 3-4 to 4-5', 'swap Thu 9-11 with Jane Doe'.")
