import streamlit as st
import pandas as pd
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from ui_badges import display_badge

def _load_real_error_log():
    """Load real error log from api_manager. Returns a display-ready DataFrame."""
    try:
        from api_manager import get_error_log
        records = get_error_log(n=200)
    except Exception:
        records = []

    if not records:
        return pd.DataFrame(columns=["Timestamp (IST)", "Severity", "Component", "Message", "Auto-Fix?", "Status", "Notes"])

    rows = []
    for r in records:
        auto_fix = "✅ Yes" if r.get("auto_fixed") else "❌ No"
        status_raw = r.get("status", "Open")
        if status_raw == "Auto-Fixed":
            status = "✅ Fixed"
        elif status_raw == "Open":
            status = "🔴 Open"
        elif status_raw == "Suppressed":
            status = "⚫ Suppressed"
        else:
            status = f"⚠️ {status_raw}"
        rows.append({
            "Timestamp (IST)": r.get("datetime_ist", ""),
            "Severity": r.get("severity", ""),
            "Component": r.get("component", ""),
            "Message": r.get("message", ""),
            "Auto-Fix?": auto_fix,
            "Status": status,
            "Notes": r.get("resolution_notes", r.get("root_cause", "")),
        })
    return pd.DataFrame(rows)


def render():
    display_badge("errors")
    st.markdown("### 🐞 Developer Ops & Bug / Error Tracker")
    st.info("Live feed from `api_manager.py` error log. Every API fault, fallback, and auto-heal is stamped here in real time.")

    st.markdown("---")

    df = _load_real_error_log()

    # Metrics
    c1, c2, c3 = st.columns(3)
    p0_open = len(df[(df["Severity"].str.contains("P0", na=False)) & (df["Status"].str.contains("Open", na=False))]) if not df.empty else 0
    auto_heals = len(df[df["Auto-Fix?"].str.startswith("✅", na=False)]) if not df.empty else 0
    total = len(df)
    c1.metric("Total Logged Errors", str(total))
    c2.metric("P0 Open Bugs", str(p0_open), "All Clear" if p0_open == 0 else f"⚠️ {p0_open} Open")
    c3.metric("Auto-Heals Logged", str(auto_heals))

    st.markdown("#### System Log Archive")

    filter_val = st.selectbox("Severity Filter", ["All", "P0", "P1", "P2"])

    if df.empty:
        st.info("No errors logged yet. Errors will appear here as the system runs.")
    else:
        if filter_val != "All":
            df = df[df["Severity"].str.contains(filter_val, na=False)]
        st.dataframe(df, use_container_width=True, hide_index=True)
