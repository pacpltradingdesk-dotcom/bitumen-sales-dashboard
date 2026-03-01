import streamlit as st
import pandas as pd
import datetime
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from ui_badges import display_badge

def _load_real_change_log():
    """Load real change log from api_manager. Returns a display-ready DataFrame."""
    try:
        from api_manager import get_change_log
        records = get_change_log(n=500)
    except Exception:
        records = []

    if not records:
        return pd.DataFrame(columns=["Date-Time (IST)", "Field Changed", "Old Value", "New Value", "Location", "Reason", "Changed By", "Impacted Areas"])

    rows = []
    for r in records:
        trigger = r.get("trigger", "Auto")
        changed_by = f"🤖 {r.get('user_id', 'System')}" if trigger == "Auto" else r.get("user_id", "Manual")
        rows.append({
            "Date-Time (IST)": r.get("datetime_ist", ""),
            "Field Changed": r.get("what_changed", ""),
            "Old Value": r.get("old_value", ""),
            "New Value": r.get("new_value", ""),
            "Location": r.get("component", ""),
            "Reason": r.get("reason", ""),
            "Changed By": changed_by,
            "Impacted Areas": r.get("affected_tab", r.get("affected_api", "")),
        })
    return pd.DataFrame(rows)


def render():
    display_badge("notifications")
    st.markdown("### 🔔 System Activity & Change Log")
    st.info("Live feed from `api_manager.py` change log. Every number modification (Auto or Manual) across the Bitumen Dashboard is stamped here in IST.")

    st.markdown("---")

    df = _load_real_change_log()

    c1, c2 = st.columns(2)
    filter_val = c1.selectbox("Filter History Logs By:", ["All Changes", "🤖 Auto Updates Only", "👤 Manual Overrides Only"])
    date_range = c2.date_input(
        "Filter Date Range",
        value=(datetime.date.today() - datetime.timedelta(days=7), datetime.date.today())
    )

    st.markdown("#### Audit Trail")

    if df.empty:
        st.info("No changes logged yet. Changes will appear here as the system runs.")
        return

    if "Auto" in filter_val:
        df = df[df["Changed By"].str.contains("🤖", na=False)]
    elif "Manual" in filter_val:
        df = df[~df["Changed By"].str.contains("🤖", na=False)]

    # Date filter
    if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
        start_d, end_d = date_range
        def _parse_date(s):
            try:
                return pd.to_datetime(s, dayfirst=True).date()
            except Exception:
                return None
        df["_date"] = df["Date-Time (IST)"].apply(_parse_date)
        df = df[(df["_date"] >= start_d) & (df["_date"] <= end_d)]
        df = df.drop(columns=["_date"])

    st.dataframe(df, use_container_width=True, hide_index=True)
