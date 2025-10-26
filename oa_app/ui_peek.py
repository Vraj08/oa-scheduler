import pandas as pd
import streamlit as st
import re
import gspread.utils as a1
from datetime import datetime
from dateutil import parser as dateparser
from .config import ONCALL_MAX_COLS, ONCALL_MAX_ROWS
from .quotas import read_cols_exact, _safe_batch_get

def peek_exact(schedule, tab_titles: list[str]):
    # Renders MC/UNH style (Mon–Sun header) sheets.
    # Creates its own expander; don't wrap it in another one.
    with st.expander("Peek (exactly as in sheet)"):
        tab = st.selectbox("Campus tab", tab_titles, index=0, key="peek_tab_raw")
        info = schedule._get_sheet(tab.split()[0])
        view_mode = st.radio("View", ["Selected day", "All days"], horizontal=True, key="peek_view_raw")
        max_rows = st.number_input("Max rows to show (0 = all)", min_value=0, value=0, step=1, key="peek_rows_raw")

        total_rows = info.ws.row_count or 2000
        start_r = 2 if total_rows >= 2 else 1
        end_r = total_rows

        if view_mode == "Selected day":
            day = st.selectbox("Day", [d.title() for d in sorted(info.header_map.keys())], key="peek_day_raw")
            if day.lower() not in info.header_map:
                st.info("Could not find that day in this worksheet.")
            else:
                cols = [1, info.header_map[day.lower()]]
                data = read_cols_exact(info.ws, start_r, end_r, cols)
                out_time, out_day = data[1], data[cols[1]]
                if max_rows and max_rows > 0:
                    out_time = out_time[:max_rows]; out_day = out_day[:max_rows]
                st.dataframe(pd.DataFrame({"Time": out_time, day.title(): out_day}), height=520, width='stretch')
        else:
            order = ["monday","tuesday","wednesday","thursday","friday","saturday","sunday"]
            day_cols = [(d, info.header_map[d]) for d in order if d in info.header_map]
            if not day_cols:
                st.info("No weekday headers detected in this worksheet.")
            else:
                cols = [1] + [c for _, c in day_cols]
                data = read_cols_exact(info.ws, start_r, end_r, cols)
                out = {"Time": data[1]}
                for d, c in day_cols:
                    out[d.title()] = data[c]
                if max_rows and max_rows > 0:
                    for k in list(out.keys()): out[k] = out[k][:max_rows]
                st.dataframe(pd.DataFrame(out), height=520, width='stretch')

def peek_oncall(ss):
    # Multi-select viewer for any visible On-Call sheets (kept for your existing flows).
    with st.expander("Peek On-Call (weekly sheets, as-is)"):
        try:
            all_ws = ss.worksheets()
        except Exception as e:
            st.warning(f"Could not list worksheets: {e}")
            return
        oc_ws = [w for w in all_ws if re.search(r"\bon\s*[- ]?\s*call\b", w.title, flags=re.I)]
        if not oc_ws:
            st.info("No visible On-Call worksheets found.")
            return

        def _parse_title_date(t: str):
            try:
                m = re.search(r"(\d{4}[-/]\d{1,2}[-/]\d{1,2}|[A-Za-z]{3,9}\s+\d{1,2}(?:,\s*\d{4})?)", t)
                if m: return dateparser.parse(m.group(1))
            except Exception:
                return None
            return None

        oc_ws.sort(key=lambda w: (_parse_title_date(w.title) or datetime.min, w.title), reverse=True)
        titles = [w.title for w in oc_ws]
        sel = st.selectbox("On-Call sheet", titles, index=0, key="oncall_sel")
        ws = next(w for w in oc_ws if w.title == sel)

        end_col = a1.rowcol_to_a1(1, ONCALL_MAX_COLS).split("1")[0]
        vals = _safe_batch_get(ws, [f"A1:{end_col}{ONCALL_MAX_ROWS}"])[0]
        if not vals:
            st.info("This On-Call worksheet is empty."); return
        hdr = vals[0] if vals else []
        body = vals[1:] if len(vals) > 1 else []
        if any((c or "").strip() for c in hdr):
            w = len(hdr)
            norm = [r + [""] * (w - len(r)) if len(r) < w else r[:w] for r in body]
            df = pd.DataFrame(norm, columns=hdr)
        else:
            df = pd.DataFrame(vals)
        st.dataframe(df, height=520, width='stretch')

def peek_oncall_single(ss, title: str):
    # Focused viewer for exactly one On-Call sheet (used when user selects a single tab in sidebar).
    with st.expander(f"Peek (On-Call): {title}"):
        try:
            ws = ss.worksheet(title)
        except Exception as e:
            st.warning(f"Could not open worksheet '{title}': {e}")
            return

        end_col = a1.rowcol_to_a1(1, ONCALL_MAX_COLS).split("1")[0]
        vals = _safe_batch_get(ws, [f"A1:{end_col}{ONCALL_MAX_ROWS}"])[0]
        if not vals:
            st.info("This On-Call worksheet is empty."); return

        hdr = vals[0] if vals else []
        body = vals[1:] if len(vals) > 1 else []

        if any((c or "").strip() for c in hdr):
            w = len(hdr)
            norm = [r + [""] * (w - len(r)) if len(r) < w else r[:w] for r in body]
            df = pd.DataFrame(norm, columns=hdr)
        else:
            df = pd.DataFrame(vals)

        st.dataframe(df, height=520, width='stretch')
