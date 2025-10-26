from datetime import datetime, timezone
from gspread import WorksheetNotFound
import gspread
from gspread.exceptions import APIError
import time as _pytime
from .config import LOCKS_SHEET
from .quotas import _safe_batch_get

def _retry_429(fn, *args, retries: int = 5, backoff: float = 0.8, **kwargs):
    for i in range(retries):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            s = str(e).lower()
            if "429" in s or "quota exceeded" in s:
                _pytime.sleep(backoff * (2 ** i))
                continue
            raise
    return fn(*args, **kwargs)

def get_or_create_locks_sheet(ss) -> "gspread.Worksheet":
    try:
        return _retry_429(ss.worksheet, LOCKS_SHEET)
    except WorksheetNotFound:
        pass
    try:
        ws = _retry_429(ss.add_worksheet, title=LOCKS_SHEET, rows=2000, cols=6)
    except APIError as e:
        if "already exists" in str(e).lower():
            ws = _retry_429(ss.worksheet, LOCKS_SHEET)
        else:
            raise
    try:
        block = _safe_batch_get(ws, ["A1:F1"])[0]
        if not (block and block[0] and any(c.strip() for c in block[0])):
            _retry_429(ws.update, range_name="A1:F1",
                       values=[["Key","Actor","ISOTime","Status","Row","Notes"]])
    except Exception:
        pass
    return ws

def lock_key(ws_title: str, day: str, start_str: str, end_str: str) -> str:
    return f"{ws_title}|{day.lower()}|{start_str}-{end_str}"

def acquire_fcfs_lock(locks_ws, key: str, actor: str, ttl_sec: int = 90) -> tuple[bool, int]:
    now = datetime.now(timezone.utc).isoformat()
    _retry_429(locks_ws.append_row, [key, actor, now, "pending", "", ""], value_input_option="RAW")
    vals = _retry_429(locks_ws.get_all_values)
    rows = vals[1:] if len(vals) > 1 else []
    cutoff = datetime.now(timezone.utc).timestamp() - ttl_sec
    claims = []
    for idx, r in enumerate(rows, start=2):
        k = r[0] if len(r) > 0 else ""
        a = r[1] if len(r) > 1 else ""
        t = r[2] if len(r) > 2 else ""
        try:
            ts = datetime.fromisoformat(t).timestamp()
        except Exception:
            continue
        if k == key and ts >= cutoff:
            claims.append((idx, a, ts))
    if not claims:
        return False, -1
    claims.sort(key=lambda x: x[2])
    winner_row = claims[0][0]
    is_winner = (claims[0][1] == actor)
    try:
        my_row = max(i for (i, a, _) in claims if a == actor)
        _retry_429(locks_ws.update, range_name=f"D{my_row}", values=[["won" if is_winner else "lost"]])
    except Exception:
        pass
    return is_winner, winner_row
