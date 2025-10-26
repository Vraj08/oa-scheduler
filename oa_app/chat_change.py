# oa_app/chat_change.py
from __future__ import annotations
from datetime import datetime
from typing import Optional, Sequence, Tuple, List

from .utils import fmt_time
from .chat_add import handle_add
from .chat_remove import handle_remove

def _to_list(x):
    # Accept scalars or sequences; return a list (None stays None)
    if x is None:
        return None
    if isinstance(x, (list, tuple)):
        return list(x)
    return [x]

def _zip_strict(*lists):
    # Like zip, but ensures all lists have same length
    if not lists:
        return []
    n = len(lists[0])
    if any(len(L) != n for L in lists):
        raise ValueError("Mismatched list lengths for multi-change request.")
    return zip(*lists)

def handle_change(
    st, ss, schedule, *,
    actor_name: str,
    canon_target_name: str,
    campus_title: str,
    # Back-compat: older callers pass only `day`, `old_start`, `old_end`, `new_start`, `new_end`.
    # Cross-day + multi-change: support `old_day`/`new_day` and sequences for days/times.
    day: Optional[str] = None,                 # legacy: original day if cross-day not specified
    old_day: Optional[Sequence[str]] | Optional[str] = None,
    new_day: Optional[Sequence[str]] | Optional[str] = None,
    old_start: Sequence[datetime] | datetime = None,
    old_end:   Sequence[datetime] | datetime = None,
    new_start: Sequence[datetime] | datetime = None,
    new_end:   Sequence[datetime] | datetime = None,
) -> str:
    """
    Change shift(s) by removing existing window(s) and adding new window(s).

    • Uses the same add/remove handlers you already have (no logic changes).
    • Supports cross-day changes (e.g., 'Mon 8 AM–9 PM → Wed 9 AM–11 PM').
    • Supports multiple changes in one call by passing lists of days/times.
    • Transactional per pair: if 'add' fails, the original window is restored.

    Accepted inputs (any of these):
      1) Single change, same day:
         day="monday", old_start=…, old_end=…, new_start=…, new_end=…
      2) Single change, cross-day:
         old_day="monday", new_day="wednesday", old_start=…, old_end=…, new_start=…, new_end=…
      3) Multiple changes:
         old_day=[…], new_day=[…], old_start=[…], old_end=[…], new_start=[…], new_end=[…]
    """
    # Normalize inputs to lists
    old_day_list = _to_list(old_day) if old_day is not None else None
    new_day_list = _to_list(new_day) if new_day is not None else None

    old_start_list = _to_list(old_start)
    old_end_list   = _to_list(old_end)
    new_start_list = _to_list(new_start)
    new_end_list   = _to_list(new_end)

    # Legacy path: only `day` provided for both old/new day
    if old_day_list is None and new_day_list is None:
        if day is None:
            raise ValueError("Missing day information for change.")
        old_day_list = [day]
        new_day_list = [day]

    # If only one side of day lists was given, mirror it
    if old_day_list is None and new_day_list is not None:
        old_day_list = list(new_day_list)
    if new_day_list is None and old_day_list is not None:
        new_day_list = list(old_day_list)

    # Basic presence checks
    if not all([old_day_list, new_day_list, old_start_list, old_end_list, new_start_list, new_end_list]):
        raise ValueError("Missing one or more required values for change (days/times).")

    # Ensure lengths match for multi-change
    n_pairs = len(old_day_list)
    if any(len(lst) != n_pairs for lst in [new_day_list, old_start_list, old_end_list, new_start_list, new_end_list]):
        raise ValueError("Mismatched list lengths for multi-change request.")

    summaries: List[str] = []

    # Perform each change atomically (remove → add with rollback)
    for o_day, n_day, o_s, o_e, n_s, n_e in _zip_strict(
        old_day_list, new_day_list, old_start_list, old_end_list, new_start_list, new_end_list
    ):
        # 1) Remove original
        remove_msg = handle_remove(
            st, ss, schedule,
            canon_target_name=canon_target_name,
            campus_title=campus_title,
            day=o_day,
            start=o_s,
            end=o_e,
        )

        # 2) Add new (with rollback on failure)
        try:
            _ = handle_add(
                st, ss, schedule,
                actor_name=actor_name,
                canon_target_name=canon_target_name,
                campus_title=campus_title,
                day=n_day,
                start=n_s,
                end=n_e,
            )
        except Exception as add_err:
            # Roll back: re-add the original window
            try:
                _ = handle_add(
                    st, ss, schedule,
                    actor_name=actor_name,
                    canon_target_name=canon_target_name,
                    campus_title=campus_title,
                    day=o_day,
                    start=o_s,
                    end=o_e,
                )
            except Exception as rollback_err:
                raise ValueError(
                    "Add failed and rollback also failed — schedule may be inconsistent.\n\n"
                    f"Add error: {add_err}\nRollback error: {rollback_err}"
                ) from add_err

            # Original restored; report the add error cleanly
            raise ValueError(
                f"Could not add the new window on {str(n_day).title()} "
                f"{fmt_time(n_s)}–{fmt_time(n_e)}. Your original shift was restored.\n\n"
                f"Reason: {add_err}"
            ) from add_err

        # If we get here, the pair succeeded
        summaries.append(
            f"removed **{str(o_day).title()} {fmt_time(o_s)}–{fmt_time(o_e)}**, "
            f"added **{str(n_day).title()} {fmt_time(n_s)}–{fmt_time(n_e)}**"
        )

    # Final combined message
    if len(summaries) == 1:
        return (
            f"Changed **{canon_target_name}** on **{campus_title}** — {summaries[0]}."
        )
    else:
        # Multi-change summary; join with semicolons
        return (
            f"Changed **{canon_target_name}** on **{campus_title}** — "
            + "; ".join(summaries) + "."
        )
