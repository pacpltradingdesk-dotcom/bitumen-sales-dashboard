"""
PPS Anantam — Sub-Tab Bar v4.0
================================
Horizontal sub-tab strip (max 4 tabs) rendered at the top of the content area.
Each module in the sidebar has its own set of sub-tabs defined in nav_config.py.
Tabs with "also" pages show a secondary switcher below the tab bar.
Vastu Design: NAVY #1e3a5f, GOLD #c9a84c, GREEN #2d6a4f.
"""
from __future__ import annotations

import streamlit as st
from nav_config import MODULE_NAV, get_tabs


def _inject_subtab_css():
    """Inject sub-tab bar CSS once per session."""
    if st.session_state.get("_subtab_css_done"):
        return
    st.markdown("""
<style>
/* ── Sub-Tab Bar ─────────────────────────────────────────────────────────── */
.pps-subtab-bar {
  display: flex; gap: 6px; padding: 0 0 10px 0;
  border-bottom: 2px solid #ebe6dc;
  margin-bottom: 14px;
  flex-wrap: wrap;
}
.pps-subtab {
  padding: 8px 18px; border-radius: 8px 8px 0 0;
  font-size: 0.82rem; font-weight: 600;
  color: #475569; background: transparent;
  border: 1px solid transparent;
  border-bottom: none; cursor: pointer;
  transition: all 0.15s ease;
  white-space: nowrap;
}
.pps-subtab:hover {
  background: #f5f0e8; color: #1e3a5f;
}
.pps-subtab.active {
  background: #ffffff; color: #2d6a4f;
  border: 1px solid #ebe6dc; border-bottom: 2px solid #ffffff;
  margin-bottom: -2px; font-weight: 700;
}
/* Also-page switcher */
.pps-also-bar {
  display: flex; gap: 8px; padding: 4px 0 8px 0;
  flex-wrap: wrap;
}
.pps-also-chip {
  padding: 3px 12px; border-radius: 14px;
  font-size: 0.73rem; font-weight: 500;
  color: #475569; background: #f3f0ea;
  border: 1px solid #e2ddd4; cursor: pointer;
  transition: all 0.15s;
}
.pps-also-chip:hover { background: #ebe6dc; }
.pps-also-chip.active {
  background: #e8f5ee; color: #2d6a4f;
  border-color: #b7dfc9; font-weight: 600;
}

@media (max-width: 768px) {
  .pps-subtab { padding: 6px 12px; font-size: 0.76rem; }
  .pps-also-chip { font-size: 0.68rem; padding: 2px 8px; }
}
</style>
""", unsafe_allow_html=True)
    st.session_state["_subtab_css_done"] = True


def render_subtab_bar(module: str) -> str:
    """
    Render the horizontal sub-tab bar for the given module.

    Returns the final selected_page string (exact old page name) for the
    dispatch chain in dashboard.py.
    """
    _inject_subtab_css()

    tabs = get_tabs(module)
    if not tabs:
        return "🏠 Home"

    # ── Determine active tab index ───────────────────────────────────────────
    state_key = f"_subtab_idx_{module}"
    active_idx = st.session_state.get(state_key, 0)
    if active_idx >= len(tabs):
        active_idx = 0

    # ── Render tabs as Streamlit buttons in columns ──────────────────────────
    cols = st.columns(len(tabs))
    for i, tab in enumerate(tabs):
        with cols[i]:
            btn_type = "primary" if i == active_idx else "secondary"
            if st.button(
                tab["label"],
                key=f"_stab_{module}_{i}",
                use_container_width=True,
                type=btn_type,
            ):
                st.session_state[state_key] = i
                # Reset also-page selection when switching tabs
                also_key = f"_also_{module}_{i}"
                if also_key in st.session_state:
                    del st.session_state[also_key]
                st.rerun()

    active_tab = tabs[active_idx]

    # ── Handle "also" pages (secondary switcher) ─────────────────────────────
    also_pages = active_tab.get("also", [])
    selected_page = active_tab["page"]

    if also_pages:
        all_options = [active_tab["page"]] + also_pages
        # Build display labels (strip emoji for cleaner look)
        labels = []
        for p in all_options:
            # Keep full page name as label
            labels.append(p)

        also_key = f"_also_{module}_{active_idx}"
        current_also = st.session_state.get(also_key, active_tab["page"])
        if current_also not in all_options:
            current_also = active_tab["page"]

        # Render as small radio buttons
        chosen = st.radio(
            "View:",
            options=all_options,
            index=all_options.index(current_also),
            key=also_key,
            horizontal=True,
            label_visibility="collapsed",
        )
        selected_page = chosen

    # Handle special pages
    if selected_page == "_quick_send":
        _render_quick_send()
        return "_quick_send"

    return selected_page


def _render_quick_send():
    """Inline Quick Send panel for the Home module."""
    st.markdown("### Quick Send")
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        if st.button("📧 Email", key="_qs_email", use_container_width=True):
            st.info("Navigate to any page and use the action bar to send context-rich emails.")
    with c2:
        if st.button("💬 WhatsApp", key="_qs_wa", use_container_width=True):
            st.info("Navigate to any page and use the action bar for WhatsApp messaging.")
    with c3:
        if st.button("📄 PDF Export", key="_qs_pdf", use_container_width=True):
            st.info("Navigate to any page and use the action bar to generate PDF reports.")
    with c4:
        if st.button("🖨 Print", key="_qs_print", use_container_width=True):
            st.info("Navigate to any page and use the action bar for printing.")
