"""
PPS Anantam — Top Control Bar v4.0
====================================
Sticky global control bar with status indicators and quick actions.
Rendered at the top of the main content area (not inside sidebar).
Vastu Design: NAVY #1e3a5f, GOLD #c9a84c, GREEN #2d6a4f.
"""
from __future__ import annotations

import json
from pathlib import Path
import streamlit as st

_BASE = Path(__file__).parent
_NAVY = "#1e3a5f"
_GOLD = "#c9a84c"
_GREEN = "#2d6a4f"


def _dot(color: str) -> str:
    """Small colored status dot."""
    return f'<span style="color:{color};font-size:0.7rem;">&#9679;</span>'


def _read_json(path: str, default=None):
    """Safe JSON read."""
    try:
        fp = _BASE / path
        if fp.exists():
            with open(fp, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        pass
    return default if default is not None else []


def _inject_topbar_css():
    """Inject sticky top bar CSS (once per session)."""
    if st.session_state.get("_topbar_css_done"):
        return
    st.markdown("""
<style>
/* ── Sticky Top Control Bar ─────────────────────────────────────────────── */
.pps-topbar {
  position: sticky; top: 0; z-index: 999;
  background: #ffffff;
  border-bottom: 1px solid #e2ddd4;
  box-shadow: 0 1px 4px rgba(30,58,95,0.06);
  padding: 6px 16px;
  margin: -1rem -1rem 12px -1rem;
  display: flex; align-items: center; gap: 6px;
  flex-wrap: wrap;
}
.pps-topbar-brand {
  font-size: 0.78rem; font-weight: 800; color: #1e3a5f;
  margin-right: 8px; white-space: nowrap;
}
.pps-topbar-item {
  display: inline-flex; align-items: center; gap: 4px;
  background: #f8f6f2; border: 1px solid #ebe6dc;
  border-radius: 8px; padding: 4px 10px;
  font-size: 0.72rem; font-weight: 600; color: #334155;
  white-space: nowrap; cursor: default;
  transition: background 0.15s;
}
.pps-topbar-item:hover { background: #f0ebe2; }
.pps-topbar-sep {
  width: 1px; height: 22px; background: #e2ddd4; margin: 0 4px;
}
/* Make Streamlit toggle inside top bar compact */
.pps-topbar-toggle { display: inline-flex; align-items: center; }
.pps-topbar-toggle label { font-size: 0.72rem !important; }

@media (max-width: 768px) {
  .pps-topbar { padding: 4px 8px; gap: 4px; }
  .pps-topbar-item { font-size: 0.65rem; padding: 3px 6px; }
  .pps-topbar-brand { font-size: 0.70rem; }
}
</style>
""", unsafe_allow_html=True)
    st.session_state["_topbar_css_done"] = True


def render_top_bar() -> None:
    """Render the sticky top control bar with status indicators."""
    _inject_topbar_css()

    # ── Gather live status data ──────────────────────────────────────────────
    # 1. AI status
    ai_passed, ai_total = 0, 0
    try:
        from ai_setup_engine import get_module_registry
        _reg = get_module_registry()
        if _reg:
            ai_total = len(_reg)
            ai_passed = sum(1 for m in _reg if m.get("health_test") == "pass")
    except Exception:
        pass
    if ai_total == 0:
        ai_dot, ai_text = _dot("#94a3b8"), "AI: N/A"
    elif ai_passed == ai_total:
        ai_dot, ai_text = _dot(_GREEN), f"AI: {ai_passed}/{ai_total}"
    elif ai_passed > 0:
        ai_dot, ai_text = _dot(_GOLD), f"AI: {ai_passed}/{ai_total}"
    else:
        ai_dot, ai_text = _dot("#dc2626"), "AI: 0"

    # 2. Sync status
    sync_logs = _read_json("sync_logs.json", [])
    if sync_logs:
        last_sync = sync_logs[-1] if isinstance(sync_logs, list) else {}
        sync_status = last_sync.get("status", "unknown")
        if sync_status in ("success", "completed"):
            sync_dot, sync_text = _dot(_GREEN), "Sync OK"
        elif sync_status in ("partial", "warning"):
            sync_dot, sync_text = _dot(_GOLD), "Sync Partial"
        else:
            sync_dot, sync_text = _dot("#dc2626"), "Sync Issue"
    else:
        sync_dot, sync_text = _dot("#94a3b8"), "No Sync"

    # 3. Worker status
    worker_dot, worker_text = _dot("#94a3b8"), "Workers: N/A"
    try:
        from ai_workers import get_worker_status
        ws = get_worker_status()
        running = sum(1 for w in ws if w.get("status") == "running")
        total_w = len(ws)
        if running == total_w and total_w > 0:
            worker_dot, worker_text = _dot(_GREEN), f"Workers: {running}/{total_w}"
        elif running > 0:
            worker_dot, worker_text = _dot(_GOLD), f"Workers: {running}/{total_w}"
        else:
            worker_dot, worker_text = _dot("#94a3b8"), f"Workers: 0/{total_w}"
    except Exception:
        pass

    # 4. Alerts count
    alerts = _read_json("sre_alerts.json", [])
    open_alerts = sum(1 for a in alerts if isinstance(a, dict) and a.get("status") == "Open")
    if open_alerts == 0:
        alert_dot, alert_text = _dot(_GREEN), "Alerts: 0"
    elif open_alerts <= 3:
        alert_dot, alert_text = _dot(_GOLD), f"Alerts: {open_alerts}"
    else:
        alert_dot, alert_text = _dot("#dc2626"), f"Alerts: {open_alerts}"

    # 5. User role
    try:
        from role_engine import get_current_role
        role = get_current_role()
    except Exception:
        role = "admin"

    # ── Render top bar as HTML ───────────────────────────────────────────────
    st.markdown(f"""
<div class="pps-topbar">
  <span class="pps-topbar-brand">🏛️ PPS Anantam</span>
  <div class="pps-topbar-sep"></div>
  <span class="pps-topbar-item">{ai_dot} {ai_text}</span>
  <span class="pps-topbar-item">{sync_dot} {sync_text}</span>
  <span class="pps-topbar-item">{worker_dot} {worker_text}</span>
  <span class="pps-topbar-item">{alert_dot} {alert_text}</span>
  <div class="pps-topbar-sep"></div>
  <span class="pps-topbar-item">👤 {role.title()}</span>
</div>
""", unsafe_allow_html=True)

    # ── API toggle + Quick Send (as Streamlit widgets below the HTML bar) ────
    cols = st.columns([1, 1, 1, 1, 4])
    with cols[0]:
        api_val = st.toggle(
            "🌐 APIs",
            value=st.session_state.get("_api_toggle_v3", False),
            key="_api_toggle_v3",
            help="Live Data APIs: ON = real-time, OFF = offline",
        )
    with cols[1]:
        if st.button("📧", key="_tb_email", help="Quick Email"):
            st.session_state["_tb_quick_action"] = "email"
    with cols[2]:
        if st.button("📄", key="_tb_pdf", help="Quick PDF"):
            st.session_state["_tb_quick_action"] = "pdf"
    with cols[3]:
        if st.button("🔍", key="_tb_search", help="Global Search"):
            st.session_state["_tb_quick_action"] = "search"

    # ── Handle quick action popover ──────────────────────────────────────────
    _action = st.session_state.get("_tb_quick_action")
    if _action == "search":
        with st.expander("🔍 Global Search", expanded=True):
            q = st.text_input("Search buyers, suppliers, tenders, news...",
                              key="_tb_search_q", placeholder="Type to search...")
            if q:
                _render_search_results(q)
            if st.button("Close", key="_tb_close_search"):
                st.session_state["_tb_quick_action"] = None
                st.rerun()
    elif _action == "email":
        with st.expander("📧 Quick Email", expanded=True):
            st.info("Use the action bar on any page to send emails with page context.")
            if st.button("Close", key="_tb_close_email"):
                st.session_state["_tb_quick_action"] = None
                st.rerun()
    elif _action == "pdf":
        with st.expander("📄 Quick PDF Export", expanded=True):
            st.info("Use the action bar on any page to export PDFs with page data.")
            if st.button("Close", key="_tb_close_pdf"):
                st.session_state["_tb_quick_action"] = None
                st.rerun()


def _render_search_results(query: str) -> None:
    """Basic global search across key data sources."""
    query_lower = query.lower()
    results = []

    # Search suppliers/customers in database
    try:
        from database import get_all_suppliers, get_all_customers
        for s in get_all_suppliers():
            name = s.get("name", "")
            if query_lower in name.lower():
                results.append(f"**Supplier:** {name} — {s.get('city', '')}")
        for c in get_all_customers():
            name = c.get("name", "")
            if query_lower in name.lower():
                results.append(f"**Customer:** {name} — {c.get('city', '')}")
    except Exception:
        pass

    # Search news
    try:
        news = _read_json("tbl_news_feed.json", [])
        for n in news[-50:]:
            headline = n.get("headline", "")
            if query_lower in headline.lower():
                results.append(f"**News:** {headline[:80]}")
    except Exception:
        pass

    if results:
        for r in results[:10]:
            st.markdown(r)
        if len(results) > 10:
            st.caption(f"...and {len(results) - 10} more results")
    else:
        st.caption("No results found.")
