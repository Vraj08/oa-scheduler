from __future__ import annotations
import os, sys, re, time as _pytime
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import streamlit as st
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime

# schedule + rendering
from oa_app.schedule_query import (
    chat_schedule_response,
    get_user_schedule,
    render_user_schedule_markdown,
    build_schedule_dataframe,
    render_schedule_viz,
)

from oa_app.config import (
    DEFAULT_SHEET_URL, ROSTER_SHEET, ROSTER_NAME_COLUMN_HEADER,
    AUDIT_SHEET, LOCKS_SHEET,
)
from oa_app.schedule import Schedule
from oa_app.utils import name_key, fmt_time
from oa_app.intents import parse_intent
from oa_app.ui_peek import peek_exact, peek_oncall
from oa_app.quotas import _safe_batch_get  # used by peek; leave as-is

# Hours helpers
from oa_app.hours import (
    compute_hours_fast,
    invalidate_hours_caches,
    total_hours_from_unh_mc_and_neighbor,
)

# NEW: chat action handlers (you created these)
from oa_app.chat_add import handle_add as do_add
from oa_app.chat_remove import handle_remove as do_remove
from oa_app.chat_change import handle_change as do_change
from oa_app.chat_swap import handle_swap as do_swap

# --- add near the top (imports already exist in your app.py) ---
import time, random
from gspread.exceptions import APIError

def _with_backoff(fn, *args, **kwargs):
    """Exponential backoff + jitter for gspread calls (handles 429/5xx)."""
    base = 0.6
    for i in range(6):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            sc = getattr(getattr(e, "response", None), "status_code", None)
            if isinstance(e, APIError) and (sc in (429, 500, 502, 503, 504)):
                if i == 5:
                    raise
                time.sleep(base*(2**i) + random.uniform(0, 0.4))
            else:
                # textual 429/“quota exceeded” fallbacks
                s = str(e).lower()
                if ("429" in s or "quota exceeded" in s) and i < 5:
                    time.sleep(base*(2**i) + random.uniform(0, 0.4))
                else:
                    raise

# keep your get_gspread_client() as-is

# near the top of app.py (keep this one; delete any later duplicate)
import time, random
from gspread.exceptions import APIError

@st.cache_resource(show_spinner=False)
def get_gspread_client() -> gspread.Client:
    creds_dict = dict(st.secrets.get("gcp_service_account", {}))  # type: ignore
    if not creds_dict:
        st.error("Missing service account in secrets (gcp_service_account).")
        st.stop()
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive.readonly",
    ]
    credentials = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    return gspread.authorize(credentials)

def _with_backoff(fn, *args, **kwargs):
    base = 0.6
    for i in range(6):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            sc = getattr(getattr(e, "response", None), "status_code", None)
            if isinstance(e, APIError) and (sc in (429, 500, 502, 503, 504)):
                if i == 5:
                    raise
                time.sleep(base*(2**i) + random.uniform(0, 0.4))
            else:
                s = str(e).lower()
                if ("429" in s or "quota exceeded" in s) and i < 5:
                    time.sleep(base*(2**i) + random.uniform(0, 0.4))
                else:
                    raise

@st.cache_resource(show_spinner=False)
def open_spreadsheet(spreadsheet_url: str) -> gspread.Spreadsheet:
    client = get_gspread_client()
    return _with_backoff(client.open_by_url, spreadsheet_url)

# ---------- helpers ----------
def _retry_429(fn, *args, retries: int = 5, backoff: float = 0.8, **kwargs):
    """Generic retry helper for 429 bursts on gspread ops that aren't using quotas.py."""
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

@st.cache_resource(show_spinner=False)
def get_gspread_client() -> gspread.Client:
    creds_dict = dict(st.secrets.get("gcp_service_account", {}))  # type: ignore
    if not creds_dict:
        st.error("Missing service account in secrets (gcp_service_account).")
        st.stop()
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive.readonly",
    ]
    credentials = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    return gspread.authorize(credentials)


@st.cache_data(show_spinner=False)
def load_roster(sheet_url: str) -> list[str]:
    """Read the hired OA names from the roster sheet (single cold read; 429-safe)."""
    ss_local = open_spreadsheet(sheet_url)
    try:
        ws = _retry_429(ss_local.worksheet, ROSTER_SHEET)
    except Exception:
        return []
    try:
        values = _retry_429(ws.get_all_records)
    except Exception:
        return []
    return [
        row.get(ROSTER_NAME_COLUMN_HEADER, "").strip()
        for row in values
        if isinstance(row.get(ROSTER_NAME_COLUMN_HEADER), str)
        and row.get(ROSTER_NAME_COLUMN_HEADER).strip()
    ]

def get_canonical_roster_name(input_name: str, roster_canon_by_key: dict) -> str:
    key = name_key(input_name or "")
    if not key or key not in roster_canon_by_key:
        raise ValueError("Your name is not in the hired OA list. Please use the exact name from the roster sheet.")
    return roster_canon_by_key[key]

# ---- Lazy audit (create only when needed) --------------------------------
def _get_or_create_audit_sheet_lazy(ss) -> gspread.Worksheet:
    """Create/open and cache the Audit sheet only when we actually need to log."""
    if "AUDIT_WS" in st.session_state and st.session_state["AUDIT_WS"] is not None:
        return st.session_state["AUDIT_WS"]
    try:
        ws = _retry_429(ss.worksheet, AUDIT_SHEET)
    except gspread.WorksheetNotFound:
        ws = _retry_429(ss.add_worksheet, title=AUDIT_SHEET, rows=2000, cols=10)
        _retry_429(ws.update, range_name="A1:H1",
                   values=[["Timestamp","Actor","Action","Campus","Day","Start","End","Details"]])
    st.session_state["AUDIT_WS"] = ws
    return ws

def _log(ss, actor: str, action: str, campus: str, day: str, start, end, details: str):
    """Append a row to Audit (lazy open + minimal API calls)."""
    try:
        audit_ws = _get_or_create_audit_sheet_lazy(ss)
        _retry_429(
            audit_ws.append_row,
            [
                datetime.now().isoformat(timespec="seconds"),
                actor,
                action,
                campus,
                day.title(),
                fmt_time(start),
                fmt_time(end),
                details,
            ],
            value_input_option="RAW",
        )
    except Exception:
        st.toast("Note: logging skipped due to quota.", icon="⚠️")

# ---- hours cache busting ----
def _bust_hours_cache():
    """Force the sidebar hours metric to recompute immediately."""
    try:
        compute_hours_fast.clear()  # type: ignore[attr-defined]
    except Exception:
        pass
    st.session_state["HOURS_EPOCH"] = st.session_state.get("HOURS_EPOCH", 0) + 1


# ---------- page ----------
st.set_page_config(page_title="OA Schedule Chatbot", page_icon="🗓️", layout="wide")
st.title("🗓️ OA Schedule Chatbot")
st.caption("OA's can chat and edit schedule here. The selected tab in the sidebar is the target for all actions + peek.")

SHEET_URL = st.secrets.get("SHEET_URL", DEFAULT_SHEET_URL)
if not SHEET_URL:
    st.error("Missing SHEET_URL in secrets and no DEFAULT_SHEET_URL set.")
    st.stop()

ss = open_spreadsheet(SHEET_URL)
schedule = Schedule(ss)
# make Spreadsheet handle available to other modules' caches
st.session_state.setdefault("_SS_HANDLE_BY_ID", {})[ss.id] = ss

# Roster identity check + canonicalization
roster = load_roster(SHEET_URL)
roster_keys = {name_key(n) for n in roster}
roster_canon_by_key = {name_key(n): n for n in roster}

# init hours epoch
st.session_state.setdefault("HOURS_EPOCH", 0)

# ---------- sidebar ----------
@st.cache_data(ttl=60, show_spinner=False)
def list_tabs_for_sidebar(_ss) -> list[str]:
    """All visible user tabs except the very first tab and admin tabs (Audit/_Locks). (cached)"""
    try:
        worksheets = _retry_429(_ss.worksheets)
    except Exception as e:
        st.error(f"Could not list worksheets: {e}")
        return []
    if not worksheets:
        return []
    rest = worksheets[1:]  # exclude first tab (cover)
    deny = {AUDIT_SHEET.strip().lower(), LOCKS_SHEET.strip().lower()}
    out = []
    for ws in rest:
        try:
            hidden = bool(getattr(ws, "hidden"))
        except Exception:
            hidden = bool(getattr(ws, "_properties", {}).get("hidden", False))
        if hidden:
            continue
        if ws.title.strip().lower() in deny:
            continue
        out.append(ws.title)
    return out

with st.sidebar:
    st.subheader("Who are you?")
    oa_name = st.text_input("Your full name (must match hired OA list)")

    # Hours metric — use cached fast counter; re-compute when HOURS_EPOCH changes
    if oa_name:
        key = name_key(oa_name)
        if roster and key not in roster_keys:
            st.info("Name not found in roster. Use the exact display name from the roster sheet.")
        else:
            try:
                canon = get_canonical_roster_name(oa_name, roster_canon_by_key)
                hours_now = compute_hours_fast(ss, schedule, canon, epoch=st.session_state["HOURS_EPOCH"])
                st.metric("Current hours (UNH + MC + Oncall)", f"{hours_now:.1f} / 20")
                st.progress(min(hours_now / 20.0, 1.0))
            except Exception as e:
                st.caption(f"Hours unavailable: {e}")

    st.subheader("Roster tab")
    tabs = list_tabs_for_sidebar(ss)
    if not tabs:
        st.warning("No visible tabs (except the first) found.")
        active_tab = None
    else:
        active_tab = st.selectbox("Select a tab", tabs, index=0, key="active_tab_select")

    st.session_state["active_sheet"] = active_tab

    col1, col2 = st.columns(2)
    with col1:
        if st.button("↻ Refresh tabs"):
            list_tabs_for_sidebar.clear()
            st.rerun()
    with col2:
        if st.button("🧹 Clear caches"):
            st.cache_data.clear()
            st.cache_resource.clear()
            _bust_hours_cache()
            st.rerun()

# ---------- chat ----------
if "messages" not in st.session_state:
    st.session_state.messages = [
        {"role": "assistant", "content": "Hi! Select a tab on the left, then tell me what to do: add, remove, change, or swap a shift."}
    ]

for m in st.session_state.messages:
    with st.chat_message(m["role"]):
        st.markdown(m["content"])

prompt = st.chat_input("Type your request… (e.g., add Friday 2-4pm)")

if prompt:
    st.session_state.messages.append({"role": "user", "content": prompt})
    try:
        active_tab = st.session_state.get("active_sheet")
        if not active_tab:
            raise ValueError("Select a tab in the sidebar first.")
        if roster and not (oa_name and name_key(oa_name) in roster_keys):
            raise ValueError("Your name is not in the hired OA list. Please use the exact name from the roster sheet.")

        # "my schedule" quick path
        if re.search(r"\b(schedule|my\s+schedule|what\s+are\s+my\s+shifts?)\b", prompt, flags=re.I):
            if not oa_name:
                raise ValueError("Enter your name in the sidebar first.")
            canon = get_canonical_roster_name(oa_name, roster_canon_by_key)
            md = chat_schedule_response(ss, schedule, canon)
            st.session_state.messages.append({"role": "assistant", "content": md})
            st.rerun()

        # Parse intent (day/time/name/campus) from the prompt
        intent = parse_intent(prompt, default_campus=active_tab, default_name=oa_name)

        # Canonicalize name (falls back to sidebar if not provided in prompt)
        canon = get_canonical_roster_name(intent.name or oa_name, roster_canon_by_key)
        campus = active_tab

        # Dispatch to action handlers
        if intent.kind == "add":
            # Enforces 20h cap + all 30-min slots empty (inside do_add)
            msg = do_add(
                st, ss, schedule,
                actor_name=oa_name,
                canon_target_name=canon,
                campus_title=campus,
                day=intent.day,
                start=intent.start,
                end=intent.end,
            )
            _log(ss, oa_name, "add", campus, intent.day, intent.start, intent.end, "ok")
            _bust_hours_cache()

        elif intent.kind == "remove":
            msg = do_remove(
                st, ss, schedule,
                canon_target_name=canon,
                campus_title=campus,
                day=intent.day,
                start=intent.start,
                end=intent.end,
            )
            _log(ss, oa_name, "remove", campus, intent.day, intent.start, intent.end, "ok")
            _bust_hours_cache()

        elif intent.kind == "change":
            msg = do_change(
                st, ss, schedule,
                actor_name=oa_name,
                canon_target_name=canon,
                campus_title=campus,
                day=intent.day,
                old_start=intent.old_start,
                old_end=intent.old_end,
                new_start=intent.start,
                new_end=intent.end,
            )
            _log(ss, oa_name, "change", campus, intent.day, intent.start, intent.end,
                 f"from {fmt_time(intent.old_start)}-{fmt_time(intent.old_end)}")
            _bust_hours_cache()

        elif intent.kind == "swap":
            # Currently disabled in your handler; will raise if called
            msg = do_swap()

        else:
            raise ValueError("Unknown command. Try: add Fri 2-4pm / remove Tue 11:30-1pm / change Wed from 3-4 to 4-5")

        st.session_state.messages.append({"role": "assistant", "content": f"✅ {msg}"})
    except Exception as e:
        st.session_state.messages.append({"role": "assistant", "content": f"❌ {str(e)}"})
    st.rerun()

# ---------- pictorial schedule (main pane) ----------
with st.expander("📊 Schedule (Pictorial)", expanded=False):
    oa_name = st.session_state.get("oa_name") or (oa_name if "oa_name" in locals() else "")
    if not oa_name:
        st.info("Enter your name in the sidebar to see your schedule.")
    else:
        try:
            canon = get_canonical_roster_name(oa_name, roster_canon_by_key)
            user_sched = get_user_schedule(ss, schedule, canon)
            df = build_schedule_dataframe(user_sched)

            # 1) Plotly timeline / calendar view
            render_schedule_viz(st, df, title=f"{canon} — This Week")

            # 2) Dataframe table below the graph
            from oa_app.schedule_query import render_schedule_dataframe
            render_schedule_dataframe(st, df)

        except Exception as e:
            st.error(f"Could not render pictorial schedule: {e}")

# ---------- peek ----------
active_sheet = st.session_state.get("active_sheet")
if active_sheet:
    if re.search(r"\bon\s*[- ]?call\b", active_sheet, flags=re.I):
        peek_oncall(ss)
    else:
        peek_exact(schedule, [active_sheet])
else:
    st.info("Select a roster tab on the left to peek.")
