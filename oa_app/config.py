from datetime import time

# ===== workbook config =====
DEFAULT_SHEET_URL = "https://docs.google.com/spreadsheets/d/15ZXPyZ1k2AWHpNd3WYY9XdnrY50iBpqWHIsyjmO3zd4/edit?usp=sharing"
OA_SCHEDULE_SHEETS = ["UNH (OA and GOAs)", "MC (OA and GOAs)"]
ROSTER_SHEET = "(Names of hired OAs)"
ROSTER_NAME_COLUMN_HEADER = "Name (OAs)"
AUDIT_SHEET = "Audit Log"
LOCKS_SHEET = "_Locks"   # tiny sheet for FCFS locking
ONCALL_SHEET_OVERRIDE = ""  # e.g., "On-Call (Fall Wk 2)"
# ===== guardrails =====
DAY_START = time(7, 0)
DAY_END = time(23, 59)
OA_PREFIX = "OA:"
GOA_PREFIX = "GOA:"

# ===== caching / quotas =====
DAY_CACHE_TTL_SEC = 20  # day-column cache lifetime
HEADER_MAX_COLS = 80
ONCALL_MAX_COLS = 100
ONCALL_MAX_ROWS = 1000
HOURS_DEBUG = True   # set False to silence debug prints
