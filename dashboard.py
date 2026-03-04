try:
    from india_localization import format_inr, format_inr_short, format_date, format_datetime_ist, get_financial_year, get_fy_quarter
except ImportError:
    import sys
    import os
    sys.path.append(os.path.dirname(os.path.dirname(__file__)))
    try:
        from india_localization import format_inr, format_inr_short, format_date, format_datetime_ist, get_financial_year, get_fy_quarter
    except:
        pass
import streamlit as st
from ui_badges import display_badge
import pandas as pd
import numpy as np
import urllib.parse
import datetime
from pdf_generator import create_price_pdf
from optimizer import CostOptimizer
from source_master import SOURCE_CATEGORIES, INDIAN_REFINERIES, IMPORT_TERMINALS, PRIVATE_DECANTERS, ALL_SOURCES, get_category_label
from feasibility_engine import get_feasibility_assessment, get_comparison_table, DESTINATION_COORDS, get_live_prices, save_live_prices
import os
import sales_workspace

# --- COMMAND INTELLIGENCE SYSTEM ---
from command_intel import price_prediction, import_cost_model, supply_chain
from command_intel import demand_analytics, financial_intel, gst_legal_monitor
from command_intel import risk_scoring, alert_system, strategy_panel
from command_intel import historical_revisions, manual_entry, change_log, bug_tracker
from command_intel import sre_dashboard
from command_intel import api_hub_dashboard
from command_intel import govt_hub_dashboard
from command_intel import port_tracker_dashboard
from command_intel import correlation_dashboard
from command_intel import directory_dashboard
from command_intel import director_dashboard as cmd_director_dashboard
from command_intel import daily_log_panel as cmd_daily_log
from command_intel import alert_center as cmd_alert_center
from command_intel import infra_demand_dashboard as cmd_infra_demand
from command_intel import market_signals_dashboard as cmd_market_signals
from command_intel import developer_ops_dashboard as cmd_dev_ops
from command_intel import dashboard_flow_map as cmd_flow_map
import api_dashboard

# --- PDF EXPORT SYSTEM ---
try:
    from pdf_export_bar import render_export_bar, inject_print_css
    _PDF_BAR_OK = True
except Exception:
    _PDF_BAR_OK = False
    def render_export_bar(*a, **kw): pass
    def inject_print_css(): pass

# --- UNIVERSAL ACTION BAR (Phase C) ---
try:
    from command_intel.action_bar import render_action_bar
    _ACTION_BAR_OK = True
except Exception:
    _ACTION_BAR_OK = False
    def render_action_bar(*a, **kw): pass

# --- DATA CONFIDENCE ENGINE (Phase D) ---
try:
    from data_confidence_engine import render_confidence_bar, render_source_footnote, render_data_health_card, get_overall_health
    _CONFIDENCE_OK = True
except Exception:
    _CONFIDENCE_OK = False
    def render_confidence_bar(*a, **kw): pass
    def render_source_footnote(*a, **kw): pass
    def render_data_health_card(*a, **kw): pass

# --- ROLE ENGINE (Phase C) ---
try:
    from role_engine import render_login_form, get_current_role, check_role, init_roles
    init_roles()
    _ROLE_OK = True
except Exception:
    _ROLE_OK = False
    def render_login_form(): return True
    def get_current_role(): return "admin"
    def check_role(r): return True

# --- SYSTEM STARTUP: init logging + auto health scheduler (runs once per process) ---
try:
    from api_manager import init_system, start_auto_health
    init_system()
    start_auto_health()          # background thread: health check every 30 min
except Exception:
    pass

# --- SRE ENGINE: Self-Healing + Auto Bug Fixing + Smart Alerts ---
try:
    from sre_engine import init_sre, start_sre_background
    init_sre()
    start_sre_background(interval_min=15)   # background SRE cycle every 15 min
except Exception:
    pass

# --- API HUB ENGINE: Central data integration + normalized tables ---
try:
    from api_hub_engine import init_hub, start_hub_scheduler
    init_hub()
    start_hub_scheduler(interval_min=60)    # background refresh every 60 min
except Exception:
    pass

# --- SYNC ENGINE: Master data synchronization (daily + on-demand) ---
try:
    from sync_engine import start_sync_scheduler as _start_sync_sched
    if "sync_engine_started" not in st.session_state:
        _start_sync_sched(interval_minutes=60)
        st.session_state["sync_engine_started"] = True
except Exception:
    pass

# --- EMAIL ENGINE: Background queue processor + scheduled reports ---
try:
    from email_engine import start_email_scheduler as _start_email_sched
    if "email_engine_started" not in st.session_state:
        _start_email_sched()
        st.session_state["email_engine_started"] = True
except Exception:
    pass

# --- WHATSAPP ENGINE: Background queue processor ---
try:
    from whatsapp_engine import start_whatsapp_scheduler as _start_wa_sched
    if "whatsapp_engine_started" not in st.session_state:
        _start_wa_sched()
        st.session_state["whatsapp_engine_started"] = True
except Exception:
    pass

# --- RESILIENCE: Heartbeat monitor for all daemon threads ---
try:
    from resilience_manager import HeartbeatMonitor
    HeartbeatMonitor.start_checker()
except Exception:
    pass

# --- PORT TRACKER: initialise port master + allocation rule tables ---
try:
    from port_tracker_engine import init_port_tracker
    init_port_tracker()
except Exception:
    pass

# --- PROCUREMENT DIRECTORY: seed Phase 1 entries on first run ---
try:
    from directory_engine import init_directory
    init_directory()
except Exception:
    pass


# --- MOCK CUSTOMER DATABASE (For "Search by Name" feature) ---
customer_city_map = {
    "L&T Construction": "Mumbai",
    "Tata Projects Ltd": "Pune", 
    "Dilip Buildcon": "Bhopal",
    "IRB Infrastructure": "Ahmedabad",
    "PNC Infratech": "Agra",
    "Ashoka Buildcon": "Nashik",
    "KNR Constructions": "Hyderabad",
    "NCC Limited": "Bangalore",
    "G R Infraprojects": "Udaipur",
    "Sadbhav Engineering": "Vadodara",
    "HG Infra": "Jaipur",
    "Apco Infratech": "Lucknow",
    "Montecarlo Ltd": "Patna",
    "Welspun Enterprises": "Delhi",
    "Afcons Infrastructure": "Chennai",
    "J Kumar Infra": "Mumbai",
    "Patel Engineering": "Kolkata",
    "Gayatri Projects": "Visakhapatnam",
    "Oriental Structural": "Nagpur",
    "Megha Engineering": "Hyderabad",
    "Shapoorji Pallonji": "Pune",
    "Rodic Consultants": "Ranchi",
    "VRC Constructions": "Delhi", 
    "Ceigall India": "Ludhiana"
}

# Page Config
st.set_page_config(
    page_title="PPS Anantam Agentic AI Eco System",
    page_icon="🏛️",
    layout="wide",
)

# ── Corporate Vastu Design System v3.2.3 ───────────────────────────────────
st.markdown("""
<style>
/* =========================================================================
   PPS ANANTAM AGENTIC AI ECO SYSTEM — Corporate Vastu Design System v3.2.3
   Palette: Soft ivory/sandal backgrounds · Navy headings · Vastu green/gold
   Typography: Inter / Segoe UI — clean, readable, corporate
   Mobile-first responsive + @media print
   Full UI/UX Audit: 8px numerology grid · Soft status palette · CSS tokens
   Overlap-proof layout: scoped gap, z-index hierarchy, overflow safe
   ========================================================================= */

/* ── 0. Google Fonts import ─────────────────────────────────────────────── */
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');

/* ── 1. CSS Variables ────────────────────────────────────────────────────── */
:root {
  /* Core brand colours */
  --navy:          #1e3a5f;
  --navy-mid:      #2c4f7c;
  --navy-light:    #2563eb;
  --charcoal:      #2d3142;
  --steel:         #475569;
  --steel-light:   #64748b;

  /* Vastu warm backgrounds */
  --ivory:         #faf7f2;
  --ivory-deep:    #f5f0e8;
  --cream:         #f0ebe1;
  --sandal:        #e8dcc8;
  --sandal-light:  #f2ece0;

  /* Vastu prosperity accents */
  --vastu-green:   #2d6a4f;
  --vastu-green-lt:#e8f5ee;
  --vastu-gold:    #c9a84c;
  --vastu-gold-lt: #fdf6e3;
  --vastu-fire:    #b85c38;
  --vastu-fire-lt: #fdf0eb;
  --vastu-earth:   #8c6d46;

  /* Category sidebar tints */
  --cat-sales:     rgba(45,106,79,0.07);
  --cat-ops:       rgba(30,58,95,0.07);
  --cat-finance:   rgba(201,168,76,0.09);
  --cat-legal:     rgba(184,92,56,0.07);
  --cat-strategy:  rgba(109,40,217,0.07);
  --cat-tech:      rgba(14,116,144,0.07);
  --cat-knowledge: rgba(120,53,15,0.07);
  --cat-market:    rgba(15,118,110,0.07);

  /* Shadows */
  --shadow-card:   0 2px 10px rgba(30,58,95,0.07), 0 1px 3px rgba(0,0,0,0.04);
  --shadow-hover:  0 6px 20px rgba(30,58,95,0.12), 0 2px 6px rgba(0,0,0,0.06);

  /* ── Enterprise SaaS Spacing Scale v3.2.2 ──────────────────────────────
     Mandatory scale: 2 · 4 · 8 · 12 · 16 · 20 · 24 · 32 px
     ─────────────────────────────────────────────────────────────────── */
  --s2:   2px;
  --s4:   4px;
  --s8:   8px;
  --s12: 12px;
  --s16: 16px;
  --s20: 20px;
  --s24: 24px;
  --s32: 32px;

  /* Semantic layout aliases */
  --page-gutter:    20px;   /* main content horizontal padding        */
  --section-sep:    16px;   /* vertical gap between page sections     */
  --grid-gap:       12px;   /* gap between grid/column siblings       */
  --kpi-gap:         8px;   /* gap between KPI card siblings          */
  --card-pad:       16px;   /* padding inside cards & metric boxes    */
  --table-row-h:    36px;   /* minimum table row height               */
  --sidebar-item-h: 36px;   /* minimum sidebar nav item height        */

  /* Legacy aliases kept for backward-compatibility */
  --sp-1:  var(--s8);
  --sp-2:  var(--s16);
  --sp-3:  var(--s24);
  --sp-4:  var(--s32);
  --sp-6:  48px;
  --sp-8:  64px;

  /* ── Border radius (multiples of 8px / harmonised) ──────────────────── */
  --radius-xs:  4px;
  --radius-sm:  8px;
  --radius-md: 10px;
  --radius-lg: 14px;
  --radius-xl: 20px;

  /* ── Vastu-compliant soft status colours ────────────────────────────── */
  --status-success-bg:  #f0fdf4;
  --status-success-bdr: #22c55e;
  --status-success-txt: #15803d;
  --status-warning-bg:  #fffbeb;
  --status-warning-bdr: #f59e0b;
  --status-warning-txt: #b45309;
  --status-error-bg:    #fff1f2;
  --status-error-bdr:   #f43f5e;
  --status-error-txt:   #be123c;
  --status-info-bg:     #eff6ff;
  --status-info-bdr:    #3b82f6;
  --status-info-txt:    #1d4ed8;
  --status-neutral-bg:  #f8fafc;
  --status-neutral-bdr: #94a3b8;
  --status-neutral-txt: #475569;
}

/* ── TEXT VISIBILITY CONTRACT — DO NOT VIOLATE ───────────────────────────────
   WCAG AA minimum: 4.5:1 contrast ratio for all normal text.

   TOKEN MAP — light/white/ivory backgrounds:
     --charcoal:    #2d3142  → 13.4:1  headings, nav buttons, strong UI text
     --navy:        #1e3a5f  → 12.6:1  page titles, metric values
     --steel:       #475569  →  5.5:1  labels, captions, muted text  ← MINIMUM
     --steel-light: #64748b  →  4.6:1  secondary/tertiary text (use sparingly)

   DARK NAVY backgrounds (#1e3a5f / #0f2744) — LIGHT colours CORRECT here:
     #ffffff → 9.1:1   #e2e8f0 → 7.9:1   #93c5fd → 5.0:1
     #86efac → 7.2:1   #fca5a5 → 5.4:1   #fcd34d → 9.6:1
     DO NOT change these light colours — they are correct for dark backgrounds.

   FORBIDDEN as text on light backgrounds:
     ✗ #94a3b8  (2.3:1)   ✗ #9E9E9E  (4.2:1)
     ✗ #FF9800  (4.2:1)   ✗ #FF5722  (4.5:1 borderline)

   CSS HIDING RULE — most critical, caused v3.2.3 sidebar label bug:
     ✅ CORRECT — hide SPECIFIC icon elements by their own selectors:
        summary svg { display: none; }
        summary [data-testid="stExpanderToggleIcon"] { display: none; }
     ✗ WRONG — hide a CONTAINER and try to restore children:
        summary { font-size: 0; color: transparent; }   ← destroys ALL text
        summary p { font-size: 1rem; color: #333; }     ← fragile: only works if
                                                            Streamlit uses <p>, not <span>
   RULE: Never apply display:none / font-size:0 / color:transparent to a
         container element that has text-content children.
   ✗ ALSO WRONG — color:inherit from a transparent parent:
        button > div { color: transparent; }
        button > div > p { color: inherit; }  ← inherits transparent → blank
   ✅ CORRECT — always use explicit color on restored elements:
        button > div > p { color: #2d3142 !important; }
   ─────────────────────────────────────────────────────────────────────────── */

/* ── 2. Global Typography ────────────────────────────────────────────────── */
*, *::before, *::after {
  font-family: 'Inter', 'Segoe UI', system-ui, -apple-system, sans-serif !important;
  box-sizing: border-box;
}
h1, .stMarkdown h1 {
  font-size: 1.55rem !important; font-weight: 800 !important;
  color: var(--navy) !important; line-height: 1.3 !important;
  letter-spacing: -0.3px !important;
}
h2, .stMarkdown h2 {
  font-size: 1.2rem !important; font-weight: 700 !important;
  color: var(--navy) !important; line-height: 1.4 !important;
}
h3, .stMarkdown h3 {
  font-size: 1.0rem !important; font-weight: 600 !important;
  color: var(--charcoal) !important;
}
p, li, .stMarkdown p { font-size: 0.9rem !important; line-height: 1.65 !important; color: var(--charcoal) !important; }
.stCaption, small, caption { font-size: 0.78rem !important; color: var(--steel) !important; }

/* ── 2-X  Material Icons universal neutralizer ───────────────────────────────
   Permanently hides ALL stIconMaterial/stIconEmoji spans in the sidebar.
   Uses display:none so whether the Material Icons webfont loads or not is
   completely irrelevant — the element never appears.                         */
section[data-testid="stSidebar"] [data-testid="stIconMaterial"],
section[data-testid="stSidebar"] [data-testid="stIconEmoji"] {
  display:   none !important;
  font-size: 0 !important;
  width:     0 !important;
}

/* ── 3. App Background — Zero-waste vertical space ────────────────────────── */
.stApp, [data-testid="stApp"] {
  background: var(--ivory) !important;
}
.main .block-container {
  background: transparent !important;
  padding-top: 0px !important;
  padding-bottom: var(--s12) !important;
  padding-left: var(--page-gutter) !important;
  padding-right: var(--page-gutter) !important;
  max-width: 100% !important;
  position: relative !important;
  z-index: 1 !important;
}
/* ── Kill Streamlit's built-in top padding & header whitespace ────────────── */
header[data-testid="stHeader"] {
  height: 0px !important;
  min-height: 0px !important;
  padding: 0 !important;
  background: transparent !important;
}
div[data-testid="stToolbar"] {
  display: none !important;
}
div[data-testid="stDecoration"] {
  display: none !important;
}
/* Remove gap above first element in main area */
.main .block-container > div:first-child {
  margin-top: 0 !important;
  padding-top: 0 !important;
}
/* Tighten vertical block gaps globally */
[data-testid="stVerticalBlock"] > [data-testid="stVerticalBlockBorderWrapper"],
[data-testid="stVerticalBlock"] > div {
  margin-top: 0 !important;
}
/* Reduce section separator gap globally */
.main .stMarkdown + .stMarkdown,
.main [data-testid="stVerticalBlock"] > [style*="gap"] {
  gap: 8px !important;
}
/* Remove fixed heights that waste space */
.main [data-testid="stVerticalBlock"] {
  gap: 6px !important;
}

/* ── 4. Cards / Expanders / Forms ────────────────────────────────────────── */
.stCard,
div[data-testid="stExpander"],
div[data-testid="stForm"] {
  background: #ffffff !important;
  border: 1px solid var(--sandal) !important;
  border-radius: 12px !important;
  box-shadow: var(--shadow-card) !important;
  transition: box-shadow 0.25s ease !important;
}
div[data-testid="stExpander"]:hover {
  box-shadow: var(--shadow-hover) !important;
}
div[data-testid="stMetric"] {
  transition: all 0.25s ease !important;
}
div[data-testid="stExpanderDetails"] {
  background: #ffffff !important;
}

/* ── 5. Tabs ─────────────────────────────────────────────────────────────── */
.stTabs [data-baseweb="tab-list"] {
  gap: var(--s4) !important;
  padding: var(--s4) !important;
  background: var(--cream) !important;
  border-radius: var(--radius-lg) !important;
  border: 1px solid var(--sandal) !important;
  display: flex !important;
  flex-wrap: wrap !important;
  white-space: normal !important;
  overflow: visible !important;
  height: auto !important;
}
.stTabs [data-baseweb="tab"] {
  height: 28px !important;
  min-width: fit-content !important;
  flex: 1 1 auto !important;
  border-radius: var(--radius-sm) !important;
  background: #ffffff !important;
  border: 1px solid var(--sandal) !important;
  color: var(--charcoal) !important;
  font-weight: 600 !important;
  font-size:   0.82rem !important;
  line-height: 28px !important;
  padding:     0 var(--s12) !important;
  margin-bottom: 0 !important;
  transition:  all 0.25s ease !important;
}
.stTabs [data-baseweb="tab"]:hover {
  background: #e0e7ff !important;
  color: var(--navy) !important;
  transition: all 0.25s ease !important;
}
.stTabs [data-baseweb="tab"][aria-selected="true"] {
  background: var(--navy) !important;
  color: #ffffff !important;
  border-color: var(--navy) !important;
  font-weight: 700 !important;
  box-shadow: 0 2px 8px rgba(30,58,95,0.25) !important;
}
.stTabs [data-baseweb="tab-highlight"] { display: none !important; }

/* ── 6. Metrics ──────────────────────────────────────────────────────────── */
div[data-testid="stMetric"] {
  background: #ffffff !important;
  border-radius: var(--radius-md) !important;
  padding: var(--card-pad) !important;
  border: 1px solid var(--sandal) !important;
  border-left: 4px solid var(--vastu-green) !important;
  box-shadow: var(--shadow-card) !important;
  transition: box-shadow 0.2s !important;
}
div[data-testid="stMetric"]:hover { box-shadow: var(--shadow-hover) !important; }
div[data-testid="stMetricLabel"] { color: var(--steel) !important; font-weight: 600 !important; font-size: 0.78rem !important; }
div[data-testid="stMetricValue"] { color: var(--navy) !important; font-weight: 800 !important; font-size: 1.45rem !important; }
div[data-testid="stMetricDelta"] { font-size: 0.8rem !important; font-weight: 600 !important; }

/* ── 7. Buttons ──────────────────────────────────────────────────────────── */
.stButton > button {
  background: #ffffff !important;
  border: 1px solid var(--sandal) !important;
  border-radius: var(--radius-sm) !important;
  color: var(--charcoal) !important;
  font-weight: 600 !important;
  font-size: 0.875rem !important;
  padding: var(--s8) var(--card-pad) !important;
  box-shadow: 0 1px 4px rgba(30,58,95,0.08) !important;
  transition: all 0.25s ease !important;
  min-height: var(--table-row-h) !important;
}
.stButton > button:hover {
  background: #e0e7ff !important;
  border-color: var(--navy-mid) !important;
  color: var(--navy) !important;
  box-shadow: 0 3px 8px rgba(30,58,95,0.18) !important;
  transition: all 0.25s ease !important;
}
.stButton > button:active { box-shadow: none !important; transform: translateY(0) !important; }
.stButton > button[kind="primary"] {
  background: var(--navy) !important;
  border-color: var(--navy) !important;
  color: #ffffff !important;
  font-weight: 700 !important;
  box-shadow: 0 3px 10px rgba(30,58,95,0.3) !important;
  transition: all 0.25s ease !important;
}
.stButton > button[kind="primary"]:hover {
  background: #152d4a !important;
  border-color: #152d4a !important;
  transform: translateY(-1px) !important;
}
.stButton > button[kind="secondary"] {
  background: #ffffff !important;
  border-color: var(--sandal) !important;
  color: var(--charcoal) !important;
  transition: all 0.25s ease !important;
}
.stButton > button[kind="secondary"]:hover {
  background: #e0e7ff !important;
  color: var(--navy) !important;
}

/* ── 8. Inputs / Selectbox / Sliders ─────────────────────────────────────── */
div[data-testid="stTextInput"] > div > div > input,
div[data-testid="stNumberInput"] > div > div > input,
div[data-testid="stTextArea"] > div > textarea {
  background: #ffffff !important;
  border: 1px solid var(--sandal) !important;
  border-radius: 8px !important;
  color: var(--charcoal) !important;
  font-size: 0.875rem !important;
}
div[data-testid="stSelectbox"] > div > div {
  background: #ffffff !important;
  border: 1px solid var(--sandal) !important;
  border-radius: 8px !important;
}
div[data-testid="stSlider"] div[role="slider"] {
  background: var(--vastu-green) !important;
}

/* ── 9. DataFrames / Tables ──────────────────────────────────────────────── */
div[data-testid="stDataFrame"] {
  border: 1px solid var(--sandal) !important;
  border-radius: 10px !important;
  overflow: hidden !important;
}
table { font-size: 0.82rem !important; border-collapse: collapse !important; }
th {
  background: var(--navy) !important;
  color: #ffffff !important;
  font-weight: 700 !important;
  padding: var(--s8) var(--s12) !important;
  min-height: var(--table-row-h) !important;
  position: sticky !important; top: 0 !important; z-index: 1 !important;
}
td { padding: var(--s8) var(--s12) !important; min-height: var(--table-row-h) !important; border-bottom: 1px solid var(--cream) !important; color: var(--charcoal) !important; transition: all 0.25s ease !important; }
tr:nth-child(even) td { background: var(--ivory-deep) !important; }
tr:hover td { background: #dbeafe !important; color: var(--navy) !important; }

/* ── 10. Info / Warning / Success / Error boxes ─────────────────────────── */
div[data-testid="stAlert"] {
  border-radius: var(--radius-md) !important;
  border-left-width: 4px !important;
  font-size: 0.875rem !important;
  padding: var(--s12) var(--card-pad) !important;
  margin-bottom: var(--kpi-gap) !important;
}
/* Info — calm blue */
div[data-testid="stAlert"][data-baseweb="notification"] {
  border-left-color: var(--status-info-bdr) !important;
}
/* Soft colour overlays per type by targeting the icon wrapper colour */
div[data-testid="stAlert"] > div:first-child {
  font-size: 0.875rem !important;
  line-height: 1.55 !important;
}

/* ── 11. Dividers ────────────────────────────────────────────────────────── */
hr { border-color: var(--sandal) !important; border-width: 1px 0 0 0 !important; margin: var(--section-sep) 0 !important; }
[data-testid="stDivider"] { margin: var(--s12) 0 !important; }

/* ── 12. Scrollbars (webkit) ─────────────────────────────────────────────── */
::-webkit-scrollbar { width: 6px; height: 6px; }
::-webkit-scrollbar-track { background: var(--cream); }
::-webkit-scrollbar-thumb { background: var(--sandal); border-radius: 3px; }
::-webkit-scrollbar-thumb:hover { background: var(--steel-light); }

/* ── 13. Streamlit internal spacing — SCOPED COMPRESSION ─────────────────
   Scoped to section.main so sidebar retains its own natural spacing.
   ───────────────────────────────────────────────────────────────────────── */

/* 13-A  Kill gap only in main content vertical stacks (NOT sidebar) */
section.main [data-testid="stVerticalBlock"],
.main [data-testid="stVerticalBlock"] {
  gap: 0 !important;
}
/* Sidebar vertical blocks retain a small gap between nav items */
section[data-testid="stSidebar"] [data-testid="stVerticalBlock"] {
  gap: var(--s2) !important;
}

/* 13-B  Kill border-wrapper padding in main content only */
section.main [data-testid="stVerticalBlockBorderWrapper"] {
  padding-top:    0 !important;
  padding-bottom: 0 !important;
}

/* 13-C  Each element-container in main: tiny bottom-only spacing */
section.main [data-testid="element-container"] {
  padding-top:    0 !important;
  padding-bottom: var(--s4) !important;
}

/* 13-D  Column containers: proper gap + no side-padding collapse */
[data-testid="stHorizontalBlock"] {
  gap: var(--grid-gap) !important;
  align-items: stretch !important;
}
[data-testid="column"] {
  padding-left:  0 !important;
  padding-right: 0 !important;
  min-width:     0 !important;
  overflow: hidden !important;
}

/* 13-E  Markdown paragraphs: tight line spacing in main */
section.main [data-testid="stMarkdownContainer"] > p {
  margin-top:    0 !important;
  margin-bottom: var(--s4) !important;
}
section.main [data-testid="stMarkdownContainer"] > div,
section.main [data-testid="stMarkdownContainer"] > ul,
section.main [data-testid="stMarkdownContainer"] > ol {
  margin-top:    0 !important;
  margin-bottom: var(--s4) !important;
}

/* 13-F  Widget label gaps */
[data-testid="stWidgetLabel"],
[data-testid="InputInstructions"] {
  margin-bottom: var(--s2) !important;
}

/* 13-G  Form/input bottom margin */
div[data-testid="stTextInput"],
div[data-testid="stNumberInput"],
div[data-testid="stSelectbox"],
div[data-testid="stTextArea"],
div[data-testid="stMultiSelect"],
div[data-testid="stDateInput"] {
  margin-bottom: var(--s4) !important;
}

/* 13-H  Caption / small text */
div[data-testid="stCaptionContainer"] {
  margin-top:    var(--s2) !important;
  margin-bottom: var(--s4) !important;
}

/* 13-I  Expander header vertical padding (main content only) */
section.main div[data-testid="stExpander"] > summary {
  padding-top:    var(--s8) !important;
  padding-bottom: var(--s8) !important;
}

/* 13-J  Remove top gap on first child of every page */
section.main > div > div > div:first-child { margin-top: 0 !important; }

/* 13-K  Empty placeholder divs: collapse */
[data-testid="stEmpty"] {
  height: 0 !important;
  padding: 0 !important;
  margin: 0 !important;
  overflow: hidden !important;
}

/* ── 13-Z. Z-Index Layering Hierarchy ────────────────────────────────────
   Establishes explicit stacking order so no card or tab floats above header.
   ───────────────────────────────────────────────────────────────────────── */
header[data-testid="stHeader"]                        { z-index: 100 !important; position: relative !important; }
section[data-testid="stSidebar"]                      { z-index: 90  !important; position: relative !important; }
.main .block-container                                { z-index: 1   !important; }
/* Dropdowns, tooltips, date pickers sit above all content */
[data-baseweb="popover"],
[data-baseweb="menu"],
[data-baseweb="tooltip"],
[data-testid="stDateInput"] [data-baseweb="popover"]  { z-index: 200 !important; }
/* Metric cards and expanders: relative so they stack in normal flow */
div[data-testid="stMetric"],
div[data-testid="stExpander"],
.stCard                                               { position: relative !important; z-index: 2 !important; }
/* Tab panel content sits in normal flow */
.stTabs [data-baseweb="tab-panel"]                    { position: relative !important; z-index: 1 !important; overflow: visible !important; }

/* ── 13-OV. Overflow Safety ───────────────────────────────────────────────
   Prevent any element from rendering outside its container boundary.
   ───────────────────────────────────────────────────────────────────────── */
/* Tables: horizontal scroll instead of overflow */
div[data-testid="stDataFrame"]                        { overflow-x: auto !important; }
/* Long metric values: truncate */
div[data-testid="stMetricValue"]                      { overflow: hidden !important; text-overflow: ellipsis !important; white-space: nowrap !important; }
/* Altair charts: clip to container */
div[data-testid="stVegaLiteChart"],
div[data-testid="stArrowVegaLiteChart"]               { overflow: hidden !important; }
/* Section columns: prevent bleed */
section.main [data-testid="column"]                   { overflow: hidden !important; }

/* ── 14. Utility / Helper Classes — Vastu Design Tokens ─────────────────── */
/* Section title label */
.pps-section-title {
  font-size: 0.7rem !important;
  font-weight: 700 !important;
  text-transform: uppercase !important;
  letter-spacing: 0.09em !important;
  color: var(--steel) !important;
  padding: var(--s8) 0 var(--s4) 0 !important;
  border-bottom: 1px solid var(--cream) !important;
  margin-bottom: var(--s8) !important;
  display: block !important;
}
/* Consistent status badges */
.pps-badge {
  display: inline-flex !important;
  align-items: center !important;
  padding: 3px 10px !important;
  border-radius: 20px !important;
  font-size: 0.71rem !important;
  font-weight: 700 !important;
  letter-spacing: 0.03em !important;
  line-height: 1.4 !important;
  white-space: nowrap !important;
}
.pps-badge-success { background: var(--status-success-bg) !important; color: var(--status-success-txt) !important; border: 1px solid #bbf7d0 !important; }
.pps-badge-warning { background: var(--status-warning-bg) !important; color: var(--status-warning-txt) !important; border: 1px solid #fde68a !important; }
.pps-badge-error   { background: var(--status-error-bg)   !important; color: var(--status-error-txt)   !important; border: 1px solid #fecdd3 !important; }
.pps-badge-info    { background: var(--status-info-bg)    !important; color: var(--status-info-txt)    !important; border: 1px solid #bfdbfe !important; }
.pps-badge-neutral { background: var(--status-neutral-bg) !important; color: var(--status-neutral-txt) !important; border: 1px solid #e2e8f0 !important; }
.pps-badge-gold    { background: var(--vastu-gold-lt)      !important; color: var(--vastu-earth)        !important; border: 1px solid var(--sandal) !important; }

/* KPI summary card (inline HTML cards) */
.pps-kpi-card {
  background: #ffffff !important;
  border: 1px solid var(--sandal) !important;
  border-left: 4px solid var(--vastu-green) !important;
  border-radius: var(--radius-md) !important;
  padding: var(--card-pad) !important;
  box-shadow: var(--shadow-card) !important;
}
.pps-kpi-card.gold { border-left-color: var(--vastu-gold) !important; }
.pps-kpi-card.fire { border-left-color: var(--vastu-fire) !important; }
.pps-kpi-label {
  font-size: 0.71rem !important; font-weight: 600 !important;
  color: var(--steel) !important; text-transform: uppercase !important;
  letter-spacing: 0.05em !important; margin-bottom: 4px !important;
}
.pps-kpi-value {
  font-size: 1.35rem !important; font-weight: 800 !important;
  color: var(--navy) !important; line-height: 1.2 !important;
}
.pps-kpi-delta {
  font-size: 0.78rem !important; font-weight: 600 !important; margin-top: 4px !important;
}

/* Page header (dark navy gradient) */
.pps-page-header {
  background: linear-gradient(135deg, var(--navy) 0%, #0f2744 100%) !important;
  border-radius: var(--radius-md) !important;
  padding: var(--card-pad) var(--s24) !important;
  margin-bottom: var(--section-sep) !important;
  border-bottom: 2px solid var(--vastu-gold) !important;
  box-shadow: 0 4px 16px rgba(30,58,95,0.25) !important;
}

/* Compact table override */
.pps-compact-table table { font-size: 0.79rem !important; }
.pps-compact-table th    { padding: 6px 10px !important; font-size: 0.75rem !important; }
.pps-compact-table td    { padding: 6px 10px !important; }

/* Card grid: auto-fit 2/4 column layouts on grid */
.pps-card-grid-2 { display: grid !important; grid-template-columns: repeat(2, 1fr) !important; gap: var(--grid-gap) !important; }
.pps-card-grid-4 { display: grid !important; grid-template-columns: repeat(4, 1fr) !important; gap: var(--grid-gap) !important; }
/* 3-column variant */
.pps-card-grid-3 { display: grid !important; grid-template-columns: repeat(3, 1fr) !important; gap: var(--grid-gap) !important; }

/* ── 15. Mobile responsive ───────────────────────────────────────────────── */
@media (max-width: 1024px) {
  .pps-card-grid-4 { grid-template-columns: repeat(2, 1fr) !important; }
}
@media (max-width: 768px) {
  :root {
    --page-gutter:  12px;
    --card-pad:     12px;
    --grid-gap:      8px;
    --section-sep:  12px;
    --kpi-gap:       4px;
  }
  .main .block-container { padding: 0 var(--page-gutter) var(--s12) var(--page-gutter) !important; }
  h1, .stMarkdown h1 { font-size: 1.2rem !important; line-height: 1.35 !important; }
  h2, .stMarkdown h2 { font-size: 1.0rem !important; }
  h3, .stMarkdown h3 { font-size: 0.92rem !important; }
  div[data-testid="stMetric"] { padding: var(--card-pad) !important; min-width: 0 !important; }
  div[data-testid="stMetricValue"] { font-size: 1.1rem !important; }
  div[data-testid="stMetricLabel"] { font-size: 0.72rem !important; }
  .stTabs [data-baseweb="tab-list"] { gap: var(--kpi-gap) !important; padding: var(--s8) !important; }
  .stTabs [data-baseweb="tab"] { font-size: 0.73rem !important; padding: 0 var(--s8) !important; height: 28px !important; }
  .stButton > button { font-size: 0.8rem !important; padding: var(--s8) var(--s12) !important; min-height: 32px !important; }
  table { font-size: 0.75rem !important; }
  th { padding: var(--s8) !important; }
  td { padding: 6px var(--s8) !important; }
  div[data-testid="stDataFrame"] { overflow-x: auto !important; -webkit-overflow-scrolling: touch !important; }
  .pps-card-grid-2 { grid-template-columns: 1fr !important; }
  .pps-card-grid-4 { grid-template-columns: 1fr !important; }
  hr { margin: var(--s12) 0 !important; }
}
@media (max-width: 480px) {
  :root { --page-gutter: 8px; --card-pad: 10px; }
  h1, .stMarkdown h1 { font-size: 1.05rem !important; }
  div[data-testid="stMetric"] { padding: var(--s8) !important; }
}

/* ── 16. Sidebar compact nav (Zoho-style) — v3.2.3 overlap-proof ────────────
   TRIPLE-PROTECTION against _arrow_right text bleed:
     Layer 1 — font-size:0 on summary          collapses raw text nodes
     Layer 2 — color:transparent on summary    hides any survived text colour
     Layer 3 — overflow:hidden on summary      clips anything that escapes layout
     Layer 4 — display:none on all icon spans  kills element-carried icons
   The <p> label is then restored with explicit colour + font-size + !important.
   ─────────────────────────────────────────────────────────────────────────── */

/* 16-A  Expander container: strip card styling */
section[data-testid="stSidebar"] div[data-testid="stExpander"] {
  border: none !important;
  box-shadow: none !important;
  background: transparent !important;
  border-radius: 0 !important;
  overflow: visible !important;
}
section[data-testid="stSidebar"] div[data-testid="stExpanderDetails"] {
  padding: 0 0 0 4px !important;
  background: transparent !important;
}

/* 16-B  Summary row — layout only. TEXT VISIBILITY CONTRACT compliant.
   Icons are hidden by 16-C + the 2-X global neutralizer above.
   REMOVED: font-size:0 and color:transparent (they destroyed label text). */
section[data-testid="stSidebar"] details > summary,
section[data-testid="stSidebar"] [data-testid="stExpander"] > summary {
  display:       flex !important;
  align-items:   center !important;
  gap:           0 !important;
  padding:       6px 8px 4px 8px !important;
  border-radius: 4px !important;
  cursor:        pointer !important;
  list-style:    none !important;
  min-height:    30px !important;
  position:      relative !important;
  overflow:      hidden !important;
}
/* Suppress the native <details> triangle marker */
section[data-testid="stSidebar"] details > summary::-webkit-details-marker,
section[data-testid="stSidebar"] details > summary::marker {
  display: none !important;
  content: '' !important;
}

/* 16-C  Layer 4 — hide ALL icon/arrow elements inside summary            */
/*       Targets: SVGs, Material Icon spans, toggle icon divs, any spans  */
section[data-testid="stSidebar"] details > summary svg,
section[data-testid="stSidebar"] details > summary [data-testid="stExpanderToggleIcon"],
section[data-testid="stSidebar"] details > summary [data-testid="stIconMaterial"],
section[data-testid="stSidebar"] details > summary [data-testid="stIconEmoji"],
section[data-testid="stSidebar"] details > summary > i {
  display:    none !important;        /* Layer 4 — hide element-carried icons */
  font-size:  0 !important;
  width:      0 !important;
  max-width:  0 !important;
  overflow:   hidden !important;
}

/* 16-D  Section label — style only (no recovery needed; 16-B no longer zeroes).
   Explicit color on every property — never use inherit.
   Covers <p> (older Streamlit) and <span> (current Streamlit).            */
section[data-testid="stSidebar"] details > summary p,
section[data-testid="stSidebar"] details > summary span,
section[data-testid="stSidebar"] [data-testid="stExpander"] > summary p,
section[data-testid="stSidebar"] [data-testid="stExpander"] > summary span {
  font-size:      0.7rem !important;
  color:          #2d3748 !important;   /* explicit — never inherit */
  font-weight:    700 !important;
  text-transform: uppercase !important;
  letter-spacing: 0.07em !important;
  margin:         0 !important;
  padding:        0 !important;
  line-height:    1.3 !important;
  flex:           1 1 auto !important;
  overflow:       visible !important;
  text-overflow:  clip !important;
  white-space:    normal !important;
  word-break:     break-word !important;
  max-width:      calc(100% - 20px) !important;
}

/* 16-E  Custom expand › arrow — pure CSS, zero font dependency */
section[data-testid="stSidebar"] details > summary::after {
  content:    '›' !important;
  font-size:  0.9rem !important;
  line-height: 1 !important;
  color:      #475569 !important;
  flex-shrink: 0 !important;
  margin-left: auto !important;
  padding-left: 6px !important;
  display:    inline-block !important;
  transition: transform 0.2s ease !important;
}
section[data-testid="stSidebar"] details[open] > summary::after {
  transform: rotate(90deg) !important;
}

/* 16-F  Hover state */
section[data-testid="stSidebar"] details > summary:hover {
  background: #f2ece0 !important;
}
section[data-testid="stSidebar"] details > summary:hover p,
section[data-testid="stSidebar"] details > summary:hover span,
section[data-testid="stSidebar"] [data-testid="stExpander"] > summary:hover p,
section[data-testid="stSidebar"] [data-testid="stExpander"] > summary:hover span {
  color: #1e3a5f !important;
}

/* 16-G  Nav page buttons: compact, overflow-safe */
section[data-testid="stSidebar"] .stButton > button {
  font-size:        0.78rem !important;
  color:            #2d3142 !important;   /* explicit base color — all children inherit */
  padding:          4px 8px !important;
  min-height:       32px !important;
  justify-content:  flex-start !important;
  text-align:       left !important;
  border-radius:    6px !important;
  width:            100% !important;
  margin-bottom:    2px !important;
  overflow:         hidden !important;
  text-overflow:    ellipsis !important;
  white-space:      nowrap !important;
}
section[data-testid="stSidebar"] .stButton > button[kind="primary"] {
  background:  #e8f5ee !important;
  color:       #2d6a4f !important;
  border:      1px solid #b7dfc9 !important;
  font-weight: 600 !important;
  box-shadow:  none !important;
}
section[data-testid="stSidebar"] .stButton > button[kind="secondary"] {
  background:  transparent !important;
  border:      none !important;
  color:       #2d3142 !important;
  box-shadow:  none !important;
}
section[data-testid="stSidebar"] .stButton > button[kind="secondary"]:hover {
  background: #f2ece0 !important;
  color:      #1e3a5f !important;
}

/* ── 16-H  NAV BUTTON CLEAN RENDER — TEXT VISIBILITY CONTRACT COMPLIANT ──────
   BUG FIXED: Previous Step 1 set color:transparent on button>div, then
   Step 3 used color:inherit — inheriting transparent → blank labels.

   CORRECT APPROACH: Target icon elements directly with display:none.
   NEVER set font-size:0 / color:transparent on the button container.
   ─────────────────────────────────────────────────────────────────────────── */

/* Kill icon elements by their own selectors (stable across Streamlit versions) */
section[data-testid="stSidebar"] .stButton > button [data-testid="stIconMaterial"],
section[data-testid="stSidebar"] .stButton > button [data-testid="stIconEmoji"],
section[data-testid="stSidebar"] .stButton > button [data-testid="stExpanderToggleIcon"],
section[data-testid="stSidebar"] .stButton > button svg,
section[data-testid="stSidebar"] .stButton > button i {
  display:   none !important;
  font-size: 0 !important;
  width:     0 !important;
  max-width: 0 !important;
  flex:      0 0 0 !important;
}

/* Label text — EXPLICIT color on every possible element Streamlit may use
   (p, div, span — varies by Streamlit version).
   NEVER use color:inherit here; transparent parent → invisible text.      */
section[data-testid="stSidebar"] .stButton > button p,
section[data-testid="stSidebar"] .stButton > button div,
section[data-testid="stSidebar"] .stButton > button span,
section[data-testid="stSidebar"] .stButton > button div > p,
section[data-testid="stSidebar"] .stButton > button span > p {
  display:       block !important;
  visibility:    visible !important;
  font-size:     0.78rem !important;
  color:         #2d3142 !important;   /* EXPLICIT — never inherit from container */
  white-space:   nowrap !important;
  overflow:      hidden !important;
  text-overflow: ellipsis !important;
  width:         100% !important;
  max-width:     100% !important;
  margin:        0 !important;
  padding:       0 !important;
  line-height:   1.4 !important;
  flex:          1 1 auto !important;
}

/* Selected (primary) page button: vastu green label */
section[data-testid="stSidebar"] .stButton > button[kind="primary"] p {
  color:       #2d6a4f !important;
  font-weight: 600 !important;
}

/* ── 17. Zoho CRM Dashboard — Home Page Styles ───────────────────────────── */
/* KPI bar — tighter metrics for 8-column layout */
.kpi-bar div[data-testid="stMetric"] {
  padding: var(--s8) var(--s12) !important;
  border-left-width: 3px !important;
}
.kpi-bar div[data-testid="stMetricValue"] { font-size: 1.0rem !important; }
.kpi-bar div[data-testid="stMetricLabel"] { font-size: 0.6rem !important; }
.kpi-bar div[data-testid="stMetricDelta"] { font-size: 0.65rem !important; }

/* Zoho section row header */
.zoho-row-header {
  font-size: 0.68rem !important;
  font-weight: 700 !important;
  text-transform: uppercase !important;
  letter-spacing: 0.08em !important;
  color: var(--steel) !important;
  border-bottom: 1px solid var(--sandal) !important;
  padding-bottom: var(--s4) !important;
  margin-bottom: var(--s8) !important;
  display: block !important;
}
/* Stat pill chips */
.zoho-stat-pill {
  display: inline-flex;
  align-items: center;
  gap: 4px;
  background: var(--ivory-deep);
  border: 1px solid var(--sandal);
  border-radius: 20px;
  padding: 2px 9px;
  font-size: 0.7rem;
  font-weight: 600;
  color: var(--charcoal);
  white-space: nowrap;
  margin-right: 4px;
}
/* Dashboard card with colour-accent left border */
.zoho-card {
  background: #ffffff;
  border: 1px solid var(--sandal);
  border-radius: 10px;
  padding: var(--s12) var(--card-pad);
  box-shadow: var(--shadow-card);
  height: 100%;
}
.zoho-card.green  { border-left: 4px solid var(--vastu-green); }
.zoho-card.gold   { border-left: 4px solid var(--vastu-gold); }
.zoho-card.navy   { border-left: 4px solid var(--navy); }
.zoho-card.fire   { border-left: 4px solid var(--vastu-fire); }

/* API dot indicator */
.api-dot { width:9px; height:9px; border-radius:50%; display:inline-block;
           margin-right:5px; flex-shrink:0; }
.api-dot.ok   { background:#22c55e; }
.api-dot.fail { background:#f43f5e; }
.api-dot.warn { background:#f59e0b; }

/* Alert severity row */
.alert-row {
  display: flex; align-items: flex-start; gap: 8px;
  padding: 6px 0; border-bottom: 1px solid var(--cream);
}
.alert-row:last-child { border-bottom: none; }

/* Pipeline progress bar */
.pipeline-bar {
  display: flex; height: 8px; border-radius: 4px;
  overflow: hidden; margin: 6px 0 8px 0;
}

/* ── 18. Density variant — compact mode ─────────────────────────────────── */
/* Add class="density-compact" to a wrapper or inject via JS to switch mode */
.density-compact .main .block-container { padding: var(--s12) var(--s16) !important; }
.density-compact div[data-testid="stMetric"] { padding: var(--s8) var(--s12) !important; }
.density-compact div[data-testid="stMetricValue"] { font-size: 1.1rem !important; }
.density-compact div[data-testid="stAlert"] { padding: var(--s8) var(--s12) !important; margin-bottom: var(--s4) !important; }
.density-compact hr { margin: var(--s8) 0 !important; }
.density-compact .stTabs [data-baseweb="tab"] { height: 28px !important; padding: 0 var(--s12) !important; }
.density-compact section[data-testid="stSidebar"] .stButton > button { min-height: 24px !important; padding: 2px 8px !important; }
</style>
""", unsafe_allow_html=True)

# Import Sales Calendar Helper Functions
from sales_calendar import (
    get_season_status, get_holidays_for_month, get_city_remarks,
    MONTH_NAMES
)

# --- LIVE MARKET DATA (AUTO-TICKER) ---
# --- LIVE MARKET DATA (AUTO-TICKER) ---
from market_data import get_live_market_data, get_simulated_data

# API toggle — read from session_state so the widget can live at sidebar bottom
api_active = st.session_state.get("_api_toggle_v3", False)

@st.cache_data(ttl=120)
def get_cached_market_data(is_active):
    if is_active:
        return get_live_market_data()
    else:
        return get_simulated_data()

mkt = get_cached_market_data(api_active)


@st.cache_data(ttl=300)
def _cached_db_stats():
    """Cached database stats for Home dashboard."""
    try:
        from database import get_dashboard_stats
        return get_dashboard_stats()
    except Exception:
        return {"total_suppliers": 63, "total_customers": 3, "total_deals": 0}


@st.cache_data(ttl=600)
def _cached_forecast_calendar():
    """Cached price forecast (heavy computation)."""
    try:
        from command_intel.price_prediction import generate_forecast_calendar
        return generate_forecast_calendar()
    except Exception:
        return None


@st.cache_data(ttl=300)
def _cached_market_signals():
    """Cached market intelligence signals."""
    try:
        from market_intelligence_engine import MarketIntelligenceEngine
        eng = MarketIntelligenceEngine()
        return eng.compute_all_signals()
    except Exception:
        return {}


st.markdown(f"""
<div style="
  background: var(--ivory,#faf7f2);
  border-bottom: 1px solid var(--sandal,#e8dcc8);
  padding: 6px var(--page-gutter,20px);
  margin: 0 -20px 6px -20px;
  display: flex;
  justify-content: space-between;
  align-items: center;
  font-family: 'Inter', 'Segoe UI', sans-serif;
  font-size: 0.82rem;
">
  <div style="display:flex; gap:var(--s20,20px); flex-wrap:wrap;">
    <span style="color:#1e3a5f;">
      🛢️ <b>Brent:</b> {mkt['brent']['value']}
      <span style="font-size:0.72rem;color:#475569;"> (7d: {mkt['brent']['value_7d']})</span>
      <span style="color:{mkt['brent']['color']};font-weight:600;"> {mkt['brent']['change']}</span>
    </span>
    <span style="color:#1e3a5f;">
      🛢️ <b>WTI:</b> {mkt['wti']['value']}
      <span style="font-size:0.72rem;color:#475569;"> (7d: {mkt['wti']['value_7d']})</span>
      <span style="color:{mkt['wti']['color']};font-weight:600;"> {mkt['wti']['change']}</span>
    </span>
    <span style="color:#1e3a5f;">
      💵 <b>USD/INR:</b> {mkt['usdinr']['value']}
      <span style="font-size:0.72rem;color:#475569;"> (7d: {mkt['usdinr']['value_7d']})</span>
      <span style="color:{mkt['usdinr']['color']};font-weight:600;"> {mkt['usdinr']['change']}</span>
    </span>
  </div>
  <div style="font-size:0.72rem;color:#475569;white-space:nowrap;">
    ⏱️ Live as of {mkt['timestamp']}
  </div>
</div>
""", unsafe_allow_html=True)

# ── Dual-stream news scrolling ticker ───────────────────────────────────────
try:
    from news_ticker import render_news_ticker
    render_news_ticker()
except Exception:
    pass

# ── Enterprise Header — PPS Anantam Agentic AI Eco System ───────────────────
st.markdown("""
<div style="
  background: linear-gradient(135deg, #1e3a5f 0%, #0f2744 100%);
  padding: 9px 20px;
  border-radius: 10px;
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin: 0 0 10px 0;
  box-shadow: 0 4px 14px -3px rgba(30,58,95,0.30);
  border-bottom: 2px solid #c9a84c;
">
  <div style="display:flex; align-items:center; gap:14px;">
    <span style="font-size:1.45rem;">🏛️</span>
    <div>
      <div style="font-size:1.05rem;font-weight:800;color:#ffffff;letter-spacing:-0.2px;line-height:1.25;">
        PPS Anantam Agentic AI Eco System
      </div>
      <div style="font-size:0.68rem;color:#93c5fd;letter-spacing:0.4px;margin-top:2px;">
        GST: 24AAHCV1611L2ZD &nbsp;|&nbsp; Vadodara, Gujarat &nbsp;|&nbsp; v3.2.3
      </div>
    </div>
  </div>
  <div style="display:flex; gap:16px; align-items:center;">
    <div style="background:rgba(201,168,76,0.15);padding:4px 12px;border-radius:5px;border:1px solid rgba(201,168,76,0.3);">
      <span style="color:#fcd34d;font-size:0.73rem;font-weight:600;">📍 Vadodara HQ</span>
    </div>
    <div style="text-align:right;">
      <div style="font-size:0.75rem;color:#e2e8f0;font-weight:600;">GST: 24AAHCV1611L2ZD</div>
      <div style="font-size:0.65rem;color:#b0bec5;">Auth: Director Finance</div>
    </div>
  </div>
</div>
""", unsafe_allow_html=True)

# ── Universal Page Header — consistent across all non-Home pages ─────────────
def _render_page_header(title: str, dept: str = "", badge: str = ""):
    """Renders a compact styled page title bar. Call at top of every page block."""
    import datetime as _dt2
    _today_str = _dt2.date.today().strftime("%d %b %Y")
    _badge_html = (
        f'<span style="background:#e8f5ee;color:#2d6a4f;font-size:0.68rem;font-weight:700;'
        f'padding:2px 9px;border-radius:12px;border:1px solid #b7dfc9;">{badge}</span>'
        if badge else ""
    )
    _dept_html = (
        f'<span style="font-size:0.7rem;color:#64748b;font-weight:500;">{dept}</span>'
        if dept else ""
    )
    st.markdown(f"""
<div style="display:flex;align-items:center;justify-content:space-between;
            border-bottom:2px solid var(--sandal,#e8dcc8);
            padding-bottom:6px;
            margin-bottom:10px;">
  <div style="display:flex;align-items:baseline;gap:10px;">
    <span style="font-size:1.08rem;font-weight:800;color:var(--navy,#1e3a5f);">{title}</span>
    {_dept_html}
  </div>
  <div style="display:flex;align-items:center;gap:8px;">
    {_badge_html}
    <span style="font-size:0.68rem;color:var(--steel-light,#64748b);">{_today_str}</span>
  </div>
</div>
""", unsafe_allow_html=True)

# ── INR formatter for Home page ──────────────────────────────────────────────
def _fmt_inr_home(amount) -> str:
    """Format INR with Indian comma system for homepage display."""
    try:
        amount = int(float(amount))
        s = str(abs(amount))
        if len(s) <= 3:
            formatted = s
        else:
            last3 = s[-3:]
            remaining = s[:-3]
            groups = []
            while remaining:
                groups.insert(0, remaining[-2:])
                remaining = remaining[:-2]
            formatted = ",".join(groups) + "," + last3
        sign = "-" if amount < 0 else ""
        return f"{sign}\u20b9{formatted}"
    except (ValueError, TypeError):
        return str(amount)

# Initialize Optimizer
@st.cache_resource
def get_optimizer():
    opt = CostOptimizer()
    if os.path.exists(opt.data_path):
        opt.load_data()
        return opt
    else:
        return None

optimizer = get_optimizer()

if optimizer is None:
    st.error("⚠️ Data not found! Please run the 'Data Setup' script first.")
    st.info("Run: `python convert_data.py`")
    st.stop()

# --- TABS Interface ---
# --- TABS Interface ---
# --- SIDEBAR + TOP BAR + SUB-TABS NAVIGATION (Enterprise v4.0) ---

from nav_config import MODULE_NAV, SIDEBAR_ORDER, get_default_page, get_tabs
from top_bar import render_top_bar
from subtab_bar import render_subtab_bar

# ── Sidebar CSS — Compact Enterprise v4.0 ────────────────────────────────────
st.markdown("""
<style>
/* ═══════════════════════════════════════════════════════════════════════════
   PPS Anantam — Compact Sidebar v4.0  (10 modules, no sub-items)
   ═══════════════════════════════════════════════════════════════════════════ */

/* ── Sidebar — HIDDEN (navigation moved to horizontal top bar) ────────────── */
section[data-testid="stSidebar"] {
  display: none !important;
  width: 0 !important;
  min-width: 0 !important;
}
/* Ensure main content takes full width */
.main .block-container {
  max-width: 100% !important;
  padding-left: 16px !important;
  padding-right: 16px !important;
}
button[data-testid="stSidebarCollapsedControl"] {
  display: none !important;
}

/* ── Nav buttons — compact, full text ─────────────────────────────────────── */
section[data-testid="stSidebar"] button[kind="secondary"],
section[data-testid="stSidebar"] button[kind="primary"] {
  white-space: normal !important;
  overflow: visible !important;
  text-overflow: unset !important;
  font-size: 0.84rem !important;
  font-weight: 500 !important;
  padding: 9px 14px !important;
  min-height: 44px !important;
  border-radius: 10px !important;
  text-align: left !important;
  line-height: 1.35 !important;
  transition: background 0.15s ease !important;
  border: none !important;
  margin-bottom: 1px !important;
}
section[data-testid="stSidebar"] button[kind="secondary"] {
  background: transparent !important;
  color: #334155 !important;
}
section[data-testid="stSidebar"] button[kind="secondary"]:hover {
  background: #f2ece0 !important;
  color: #1e3a5f !important;
}
/* Active module — left accent bar + green tint */
section[data-testid="stSidebar"] button[kind="primary"] {
  background: linear-gradient(90deg, #e8f5ee 0%, #f0faf4 100%) !important;
  color: #2d6a4f !important;
  font-weight: 700 !important;
  border-left: 3px solid #2d6a4f !important;
}
section[data-testid="stSidebar"] button[kind="primary"]:hover {
  background: linear-gradient(90deg, #d4edda 0%, #e8f5ee 100%) !important;
}

/* ── Sidebar dividers ─────────────────────────────────────────────────────── */
.pps-sidebar-divider {
  height: 1px; background: #e8dcc8; margin: 8px 12px;
}
/* ── Sidebar footer ───────────────────────────────────────────────────────── */
.pps-sidebar-footer {
  border-top: 1px solid #e2ddd4;
  padding: 10px 10px 6px 10px;
  margin-top: 8px;
}
.pps-sidebar-footer-info {
  font-size: 0.65rem; color: #64748b;
  padding: 3px 2px; line-height: 1.4;
}

/* ── Sub-tab bar buttons (in main content area) ──────────────────────────── */
div[data-testid="stHorizontalBlock"] button[kind="primary"] {
  background: var(--navy) !important;
  color: #ffffff !important;
  font-weight: 700 !important;
  border-bottom: 3px solid var(--vastu-gold) !important;
  box-shadow: 0 2px 8px rgba(30,58,95,0.2) !important;
  transition: all 0.25s ease !important;
}
div[data-testid="stHorizontalBlock"] button[kind="secondary"] {
  background: #ffffff !important;
  color: var(--charcoal) !important;
  border: 1px solid var(--sandal) !important;
  transition: all 0.25s ease !important;
}
div[data-testid="stHorizontalBlock"] button[kind="secondary"]:hover {
  background: #e0e7ff !important;
  color: var(--navy) !important;
}

/* ── Radio buttons (also pages) — enhanced selection ─────────────────────── */
div[data-testid="stRadio"] > div[role="radiogroup"] label {
  transition: all 0.25s ease !important;
}
div[data-testid="stRadio"] > div[role="radiogroup"] label:hover {
  background: #e0e7ff !important;
  border-radius: 14px !important;
}

/* ── RESPONSIVE ───────────────────────────────────────────────────────────── */
@media (max-width: 768px) {
  .pps-main-nav-row button {
    font-size: 0.72rem !important;
    padding: 6px 8px !important;
    min-height: 34px !important;
  }
  .pps-brand-header {
    font-size: 0.78rem !important;
    padding: 8px 12px !important;
  }
}
</style>
""", unsafe_allow_html=True)

# ── Session state initialization ─────────────────────────────────────────────
if 'selected_module' not in st.session_state:
    st.session_state['selected_module'] = '🏠 Home'
if 'selected_page' not in st.session_state:
    st.session_state['selected_page'] = '🏠 Home'

# ── HORIZONTAL MAIN NAV BAR (2-level: main tabs + sub-tabs) ──────────────────

# ── Brand header (compact, full-width) ────────────────────────────────────────
st.markdown("""
<div class="pps-brand-header" style="padding:10px 18px;margin-bottom:4px;
    background:linear-gradient(135deg,#1e3a5f 0%,#2c5282 100%);border-radius:10px;
    display:flex;align-items:center;gap:12px;justify-content:space-between;">
  <div style="display:flex;align-items:center;gap:10px;">
    <span style="font-size:1.4rem;filter:drop-shadow(0 1px 2px rgba(0,0,0,0.2));">🏛️</span>
    <div>
      <span style="font-size:0.88rem;font-weight:800;color:#ffffff;">PPS Anantam</span>
      <span style="font-size:0.68rem;font-weight:500;color:#c9a84c;margin-left:8px;">
        Agentic AI Eco System
      </span>
    </div>
  </div>
  <div style="font-size:0.58rem;color:rgba(255,255,255,0.55);">
    v4.0.0 &middot; GST: 24AAHCV1611L2ZD &middot; Vadodara HQ
  </div>
</div>
""", unsafe_allow_html=True)

# ── Main nav: 2 rows of module buttons ────────────────────────────────────────
_active_mod = st.session_state.get('selected_module', '🏠 Home')
_modules = [m for m in SIDEBAR_ORDER if not m.startswith("_")]

# Row 1: first 6 modules
_row1 = _modules[:6]
_cols1 = st.columns(len(_row1))
for _i, _mod in enumerate(_row1):
    with _cols1[_i]:
        _mod_info = MODULE_NAV.get(_mod, {})
        _btn_label = f"{_mod_info.get('icon', '')} {_mod_info.get('label', _mod)}"
        _btn_type = "primary" if _mod == _active_mod else "secondary"
        if st.button(_btn_label, key=f"_mnav_{_mod}", use_container_width=True, type=_btn_type):
            st.session_state['selected_module'] = _mod
            st.session_state['selected_page'] = get_default_page(_mod)
            st.session_state[f"_subtab_idx_{_mod}"] = 0
            st.rerun()

# Row 2: remaining modules
_row2 = _modules[6:]
_cols2 = st.columns(len(_row2))
for _i, _mod in enumerate(_row2):
    with _cols2[_i]:
        _mod_info = MODULE_NAV.get(_mod, {})
        _btn_label = f"{_mod_info.get('icon', '')} {_mod_info.get('label', _mod)}"
        _btn_type = "primary" if _mod == _active_mod else "secondary"
        if st.button(_btn_label, key=f"_mnav_{_mod}", use_container_width=True, type=_btn_type):
            st.session_state['selected_module'] = _mod
            st.session_state['selected_page'] = get_default_page(_mod)
            st.session_state[f"_subtab_idx_{_mod}"] = 0
            st.rerun()

# ── Thin separator ────────────────────────────────────────────────────────────
st.markdown('<hr style="margin:2px 0 6px 0;border:none;border-top:1px solid #e2ddd4;">', unsafe_allow_html=True)

# ── TOP CONTROL BAR (sticky, above content) ──────────────────────────────────
render_top_bar()

# ── SUB-TAB BAR (max 4 tabs per module) ──────────────────────────────────────
_current_module = st.session_state.get('selected_module', '🏠 Home')

# ── Breadcrumb ────────────────────────────────────────────────────────────────
_bc_mod_info = MODULE_NAV.get(_current_module, {})
_bc_tabs = get_tabs(_current_module)
_bc_tab_idx = st.session_state.get(f"_subtab_idx_{_current_module}", 0)
if _bc_tab_idx >= len(_bc_tabs):
    _bc_tab_idx = 0
_bc_tab_label = _bc_tabs[_bc_tab_idx]["label"] if _bc_tabs else ""
st.markdown(
    f'<div style="font-size:0.75rem;color:#64748b;padding:2px 0 4px 2px;">'
    f'{_bc_mod_info.get("icon", "")} {_bc_mod_info.get("label", "")} &gt; {_bc_tab_label}'
    f'</div>',
    unsafe_allow_html=True,
)

_subtab_page = render_subtab_bar(_current_module)
if _subtab_page and not _subtab_page.startswith("_"):
    st.session_state['selected_page'] = _subtab_page

# ── Resolve selected page ────────────────────────────────────────────────────
selected_page = st.session_state.get('selected_page', '🏠 Home')

# Logic: Only execute the selected page to prevent poor performance/crashes
# We will use 'if selected_page == "...": with st.container():' structure below.
# This variable mapping is no longer needed but kept for safety if any stray ref references it.
tab1_placeholder = st.empty()
tab_sales = st.empty()
tab2 = st.empty()
tab3 = st.empty()
tab4 = st.empty()
tab5 = st.empty()
tab6 = st.empty()
tab7 = st.empty()
tab8 = st.empty()
tab9 = st.empty()
tab10 = st.empty()
tab11 = st.empty()
tab12_sos = st.empty()


# ── RBAC Gate (Phase C) — shows login form if RBAC enabled ─────────────────
if not render_login_form():
    st.stop()

# ── Universal Print CSS + Action Bar (Phase C upgrade) ─────────────────────
inject_print_css()

# Skip action bar for Home and PDF Archive (which have their own layouts)
_NAV_HEADERS = {
    "🏠 Home",
    "📁 PDF Archive",
}
if selected_page not in _NAV_HEADERS:
    if _ACTION_BAR_OK:
        render_action_bar(
            page_name=selected_page,
            context_fn=None,
            role=get_current_role(),
        )
    elif _PDF_BAR_OK:
        render_export_bar(
            page_title=selected_page,
            role=get_current_role(),
        )

# ── Data Confidence Bar (Phase D) — shows on data-heavy pages ────────────────
_DATA_HEAVY_PAGES = {
    "🧮 Pricing Calculator", "💰 Financial Intelligence", "📊 Demand Analytics",
    "🛣️ Road Budget & Demand", "🔭 Contractor OSINT", "🔮 Price Prediction",
    "📈 Correlation Matrix", "🌍 Import Cost Model",
}
if _CONFIDENCE_OK and selected_page in _DATA_HEAVY_PAGES:
    render_confidence_bar()


# ── HOME PAGE — Executive Intelligence Dashboard ─────────────────────────────
if selected_page == "🏠 Home":
    import datetime as _dt
    import json as _json

    # ── Load data for Home dashboard ─────────────────────────────────────────
    # Price prediction data — cached
    try:
        _fc_df  = _cached_forecast_calendar()
        if _fc_df is None:
            raise ValueError("No forecast data")
        _today  = _dt.date.today()
        _future = _fc_df[_fc_df["Date"].apply(
            lambda x: x.date() if hasattr(x, "date") else x) > _today]
        if not _future.empty:
            _nr     = _future.iloc[0]
            _pred   = _nr.get("Predicted (₹/MT)", _nr.get("Predicted", 48500))
            _lo     = _nr.get("Low Range", _pred - 400)
            _hi     = _nr.get("High Range", _pred + 400)
            _rdate  = _nr.get("Revision Date", "01-04-2026")
            _status = _nr.get("Status", "Pending")
            _chart_rows = _future.head(6)
            _chart_dates  = [str(r.get("Revision Date", "")) for _, r in _chart_rows.iterrows()]
            _chart_prices = [float(r.get("Predicted (₹/MT)", r.get("Predicted", 48500)))
                             for _, r in _chart_rows.iterrows()]
        else:
            _pred, _lo, _hi, _rdate, _status = 48500, 48100, 48900, "01-04-2026", "Pending"
            _chart_dates  = ["01-04-2026", "16-04-2026", "01-05-2026", "16-05-2026", "01-06-2026", "16-06-2026"]
            _chart_prices = [48500, 48800, 49100, 48700, 49200, 49600]
    except Exception:
        _pred, _lo, _hi, _rdate, _status = 48500, 48100, 48900, "01-04-2026", "Pending"
        _chart_dates  = ["01-04-2026", "16-04-2026", "01-05-2026", "16-05-2026", "01-06-2026", "16-06-2026"]
        _chart_prices = [48500, 48800, 49100, 48700, 49200, 49600]

    # CRM tasks
    try:
        with open("crm_tasks.json", encoding="utf-8") as _f:
            _crm_tasks = _json.load(_f)
        _task_count   = len(_crm_tasks)
        _high_pri     = sum(1 for t in _crm_tasks if t.get("priority") == "High")
    except Exception:
        _crm_tasks = []
        _task_count, _high_pri = 0, 0

    # Database stats (suppliers, customers) — cached
    _db_stats = _cached_db_stats()

    # Live prices
    try:
        with open("live_prices.json", encoding="utf-8") as _f:
            _lp = _json.load(_f)
    except Exception:
        _lp = {"DRUM_MUMBAI_VG30": 37000, "DRUM_KANDLA_VG30": 35500,
               "DRUM_MUMBAI_VG10": 38000, "DRUM_KANDLA_VG10": 36500}

    # API stats
    try:
        with open("api_stats.json", encoding="utf-8") as _f:
            _api_st = _json.load(_f)
        _api_ok  = sum(1 for v in _api_st.values() if v.get("status") == "OK")
        _api_tot = len(_api_st)
    except Exception:
        _api_st, _api_ok, _api_tot = {}, 5, 6

    # Active alerts
    try:
        from command_intel.alert_system import _get_active_alerts as _gaa
        _alerts = _gaa()
        _alert_count    = len(_alerts)
        _alert_critical = sum(1 for a in _alerts if a.get("severity") == "critical")
    except Exception:
        _alerts, _alert_count, _alert_critical = [], 0, 0

    # Cheapest sources (Top 5)
    _top_sources = []
    try:
        from calculation_engine import BitumenCalculationEngine as _CalcEngine
        _calc = _CalcEngine()
        _src_cities = ["Ahmedabad", "Mumbai", "Delhi", "Pune", "Indore"]
        for _sc in _src_cities:
            try:
                _srcs = _calc.find_best_sources(_sc, grade="VG30", top_n=1)
                if _srcs:
                    _top_sources.append({
                        "city": _sc,
                        "source": _srcs[0].get("source", "Unknown"),
                        "landed": _srcs[0].get("landed_cost", 0),
                    })
            except Exception:
                pass
        _top_sources.sort(key=lambda x: x.get("landed", 99999))
    except Exception:
        pass

    # Opportunities (from opportunity engine)
    _opportunities = []
    _opp_count = 0
    try:
        from opportunity_engine import get_all_opportunities as _get_opps
        _opportunities = _get_opps(status="new")
        _opp_count = len(_opportunities)
    except Exception:
        pass

    # Today's recommendations
    _todays_recs = {}
    try:
        from opportunity_engine import OpportunityEngine as _OppEngine
        _opp_eng = _OppEngine()
        _todays_recs = _opp_eng.get_todays_recommendations()
    except Exception:
        _todays_recs = {"buyers_to_call": [], "followups_due": [], "reactivation_targets": [], "total_new_opportunities": 0}

    # Missing inputs count
    _missing_count = 0
    try:
        from missing_inputs_engine import MissingInputsEngine as _MIEngine
        _mi = _MIEngine()
        _missing_count = len(_mi.scan_all_gaps())
    except Exception:
        pass

    # ─────────────────────────────────────────────────────────────────────────
    # SECTION 1 — MARKET PULSE (Real-Time KPI Bar)
    # ─────────────────────────────────────────────────────────────────────────
    _today_str = _dt.date.today().strftime("%d %b %Y")
    st.markdown(f"""
<div style="background:linear-gradient(135deg,#1e3a5f 0%,#0f2744 100%);border-radius:10px;
            padding:12px 20px;margin-bottom:14px;display:flex;align-items:center;
            justify-content:space-between;flex-wrap:wrap;gap:8px;">
  <div style="display:flex;gap:24px;flex-wrap:wrap;align-items:center;">
    <span style="font-size:0.72rem;font-weight:800;color:#93c5fd;text-transform:uppercase;
                 letter-spacing:0.08em;">MARKET PULSE</span>
    <span style="color:#e2e8f0;font-size:0.82rem;">
      <b>Brent:</b> {mkt['brent']['value']}
      <span style="color:{mkt['brent']['color']};font-weight:600;"> {mkt['brent']['change']}</span>
    </span>
    <span style="color:#e2e8f0;font-size:0.82rem;">
      <b>WTI:</b> {mkt['wti']['value']}
      <span style="color:{mkt['wti']['color']};font-weight:600;"> {mkt['wti']['change']}</span>
    </span>
    <span style="color:#e2e8f0;font-size:0.82rem;">
      <b>INR:</b> {mkt['usdinr']['value']}
      <span style="color:{mkt['usdinr']['color']};font-weight:600;"> {mkt['usdinr']['change']}</span>
    </span>
    <span style="color:#fcd34d;font-size:0.82rem;font-weight:600;">
      VG30: {_fmt_inr_home(_lp.get("DRUM_KANDLA_VG30", 35500))}
    </span>
  </div>
  <div style="font-size:0.72rem;color:#93c5fd;white-space:nowrap;">
    {_today_str}
  </div>
</div>
""", unsafe_allow_html=True)

    # ─── P0 Alert Banner (if any critical alerts) ──────────────────────────
    try:
        from command_intel.alert_center import get_p0_alert_banner as _get_p0
        _p0_html = _get_p0()
        if _p0_html:
            st.markdown(_p0_html, unsafe_allow_html=True)
    except Exception:
        pass

    # ─────────────────────────────────────────────────────────────────────────
    # SECTION 2 — KPI Metrics (6 key metrics)
    # ─────────────────────────────────────────────────────────────────────────
    st.markdown('<div class="kpi-bar">', unsafe_allow_html=True)
    _k1, _k2, _k3, _k4, _k5, _k6, _k7 = st.columns(7)
    _k1.metric("Predicted Price", f"{_fmt_inr_home(int(_pred))}/MT")
    _k2.metric("Suppliers", _db_stats.get("total_suppliers", 63))
    _k3.metric("Customers", _db_stats.get("total_customers", 3))
    _k4.metric("Opportunities", _opp_count if _opp_count else "Scan needed")
    _k5.metric("CRM Tasks", _task_count, f"{_high_pri} high priority" if _high_pri else None)
    _k6.metric("APIs Healthy", f"{_api_ok}/{_api_tot}")
    try:
        from market_intelligence_engine import get_master_signal
        _ms = get_master_signal()
        _k7.metric("Market Signal", _ms.get("market_direction", "N/A"),
                    f"{_ms.get('confidence', 0)}% conf")
    except Exception:
        _k7.metric("Market Signal", "N/A")
    st.markdown('</div>', unsafe_allow_html=True)

    # ── Data Health Card (Phase D) ──────────────────────────────────────────
    if _CONFIDENCE_OK:
        render_data_health_card()

    # ─────────────────────────────────────────────────────────────────────────
    # SECTION 3 — Top Sources + Top Sell Opportunities (2-column)
    # ─────────────────────────────────────────────────────────────────────────
    _h3a, _h3b = st.columns(2)

    with _h3a:
        st.markdown('<div class="zoho-row-header">Top 5 Cheapest Sources (VG30 Landed)</div>', unsafe_allow_html=True)
        if _top_sources:
            _src_html = ""
            for _idx, _ts in enumerate(_top_sources[:5], 1):
                _rank_ico = ["", "1.", "2.", "3.", "4.", "5."][_idx]
                _src_html += f"""
<div style="display:flex;justify-content:space-between;align-items:center;
            padding:6px 10px;border-bottom:1px solid #f1f5f9;font-size:0.8rem;">
  <span style="color:#2d3142;font-weight:500;">{_rank_ico} {_ts['source']}</span>
  <span style="color:#1e3a5f;font-weight:700;">{_fmt_inr_home(_ts['landed'])}/MT</span>
</div>"""
            st.markdown(f'<div class="zoho-card">{_src_html}</div>', unsafe_allow_html=True)
        else:
            st.markdown("""
<div class="zoho-card" style="padding:16px;text-align:center;">
  <div style="font-size:0.82rem;color:#64748b;">Run a sync to discover cheapest sources</div>
</div>""", unsafe_allow_html=True)
        if st.button("Open Pricing Calculator", key="_home_pricing_btn", use_container_width=True):
            st.session_state["_nav_goto"] = "🧮 Pricing Calculator"
            st.rerun()

    with _h3b:
        st.markdown('<div class="zoho-row-header">Top Sell Opportunities</div>', unsafe_allow_html=True)
        _sell_opps = [o for o in _opportunities if o.get("type") == "price_drop_reactivation"][:5]
        if _sell_opps:
            _opp_html = ""
            for _idx, _so in enumerate(_sell_opps[:5], 1):
                _cname = _so.get("customer_name", "Unknown")[:25]
                _ccity = _so.get("customer_city", "")
                _margin = _so.get("estimated_margin_per_mt", 0)
                _opp_html += f"""
<div style="display:flex;justify-content:space-between;align-items:center;
            padding:6px 10px;border-bottom:1px solid #f1f5f9;font-size:0.8rem;">
  <span style="color:#2d3142;font-weight:500;">{_idx}. {_cname} ({_ccity})</span>
  <span style="color:#2d6a4f;font-weight:700;">{_fmt_inr_home(_margin)} margin</span>
</div>"""
            st.markdown(f'<div class="zoho-card">{_opp_html}</div>', unsafe_allow_html=True)
        else:
            st.markdown("""
<div class="zoho-card" style="padding:16px;text-align:center;">
  <div style="font-size:0.82rem;color:#64748b;">Run opportunity scan to find sell targets</div>
</div>""", unsafe_allow_html=True)
        if st.button("View All Opportunities", key="_home_opp_btn", use_container_width=True):
            st.session_state["_nav_goto"] = "🔍 Opportunities"
            st.rerun()

    # ─────────────────────────────────────────────────────────────────────────
    # SECTION 4 — Call Today + Negotiate Today (2-column)
    # ─────────────────────────────────────────────────────────────────────────
    _h4a, _h4b = st.columns(2)

    with _h4a:
        st.markdown('<div class="zoho-row-header">Call Today (Buyers)</div>', unsafe_allow_html=True)
        _buyers = _todays_recs.get("buyers_to_call", [])[:5]
        if _buyers:
            _buy_html = ""
            for _b in _buyers:
                _bname = _b.get("customer_name", "Unknown")[:28]
                _bcity = _b.get("customer_city", "")
                _bpri = _b.get("priority", "P2")
                _pri_clr = "#dc2626" if _bpri == "P0" else "#d97706" if _bpri == "P1" else "#3b82f6"
                _buy_html += f"""
<div style="display:flex;justify-content:space-between;align-items:center;
            padding:5px 10px;border-bottom:1px solid #f1f5f9;font-size:0.78rem;">
  <span style="color:#2d3142;font-weight:500;">{_bname}</span>
  <span style="display:flex;gap:8px;align-items:center;">
    <span style="color:#64748b;font-size:0.72rem;">{_bcity}</span>
    <span style="background:{_pri_clr};color:#fff;font-size:0.62rem;font-weight:700;
                 padding:1px 6px;border-radius:8px;">{_bpri}</span>
  </span>
</div>"""
            st.markdown(f'<div class="zoho-card">{_buy_html}</div>', unsafe_allow_html=True)
        else:
            st.markdown("""
<div class="zoho-card" style="padding:16px;text-align:center;">
  <div style="font-size:0.82rem;color:#64748b;">No buyers flagged for today. Run opportunity scan.</div>
</div>""", unsafe_allow_html=True)
        if st.button("Open CRM & Tasks", key="_home_crm_btn", use_container_width=True):
            st.session_state["_nav_goto"] = "🎯 CRM & Tasks"
            st.rerun()

    with _h4b:
        st.markdown('<div class="zoho-row-header">Follow-ups Due</div>', unsafe_allow_html=True)
        _followups = _todays_recs.get("followups_due", [])[:5]
        if _followups:
            _fu_html = ""
            for _fu in _followups:
                _fname = _fu.get("customer_name", _fu.get("title", "Task"))[:28]
                _ftype = _fu.get("type", _fu.get("channel", ""))
                _fu_html += f"""
<div style="display:flex;justify-content:space-between;align-items:center;
            padding:5px 10px;border-bottom:1px solid #f1f5f9;font-size:0.78rem;">
  <span style="color:#2d3142;font-weight:500;">{_fname}</span>
  <span style="color:#64748b;font-size:0.72rem;">{_ftype}</span>
</div>"""
            st.markdown(f'<div class="zoho-card">{_fu_html}</div>', unsafe_allow_html=True)
        else:
            _pending_tasks = [t for t in _crm_tasks if t.get("status") == "Pending"][:5]
            if _pending_tasks:
                _pt_html = ""
                for _pt in _pending_tasks:
                    _ptitle = _pt.get("title", "Task")[:28]
                    _ptype = _pt.get("type", "")
                    _pt_html += f"""
<div style="display:flex;justify-content:space-between;align-items:center;
            padding:5px 10px;border-bottom:1px solid #f1f5f9;font-size:0.78rem;">
  <span style="color:#2d3142;font-weight:500;">{_ptitle}</span>
  <span style="color:#64748b;font-size:0.72rem;">{_ptype}</span>
</div>"""
                st.markdown(f'<div class="zoho-card">{_pt_html}</div>', unsafe_allow_html=True)
            else:
                st.markdown("""
<div class="zoho-card" style="padding:16px;text-align:center;">
  <div style="font-size:0.82rem;color:#64748b;">No follow-ups due today.</div>
</div>""", unsafe_allow_html=True)
        if st.button("Open Sales Workspace", key="_home_sales_btn", use_container_width=True):
            st.session_state["_nav_goto"] = "💼 Sales Workspace"
            st.rerun()

    # ─────────────────────────────────────────────────────────────────────────
    # SECTION 5 — Price Trend Chart + Alerts (2-column)
    # ─────────────────────────────────────────────────────────────────────────
    _h5a, _h5b = st.columns([6, 4])

    with _h5a:
        st.markdown('<div class="zoho-row-header">Price Forecast (VG-30, 6-month)</div>', unsafe_allow_html=True)
        try:
            import plotly.graph_objects as _go
            _fig = _go.Figure()
            _fig.add_trace(_go.Scatter(
                x=_chart_dates, y=_chart_prices, mode='lines+markers',
                line=dict(color='#2d6a4f', width=2.5),
                marker=dict(size=6, color='#2d6a4f'),
                fill='tozeroy', fillcolor='rgba(45,106,79,0.06)',
                hovertemplate='%{x}<br>%{y:,.0f}/MT<extra></extra>'
            ))
            _fig.update_layout(
                height=200, margin=dict(l=0, r=0, t=8, b=24),
                paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
                xaxis=dict(showgrid=False, tickfont=dict(size=9), tickangle=-30),
                yaxis=dict(showgrid=True, gridcolor='#f1f5f9',
                           tickformat=',', tickfont=dict(size=9)),
                showlegend=False,
            )
            st.plotly_chart(_fig, use_container_width=True, config={"displayModeBar": False})
        except Exception:
            st.caption("Forecast chart unavailable.")
        st.markdown(f"""
<div style="display:flex;align-items:center;gap:8px;margin-top:-8px;">
  <span style="background:#e8f5ee;color:#2d6a4f;font-size:0.72rem;font-weight:700;
               padding:3px 10px;border-radius:20px;border:1px solid #b7dfc9;">
    Confidence: 82%
  </span>
  <span style="font-size:0.68rem;color:#64748b;">Next revision: {_rdate}</span>
  <span style="font-size:0.68rem;color:#1e3a5f;font-weight:600;">{_fmt_inr_home(int(_pred))}/MT</span>
</div>
""", unsafe_allow_html=True)
        if st.button("Full Forecast", key="_home_fc_btn", use_container_width=True):
            st.session_state["_nav_goto"] = "🔮 Price Prediction"
            st.rerun()

    with _h5b:
        st.markdown('<div class="zoho-row-header">Alerts</div>', unsafe_allow_html=True)
        _sev_cfg = {
            "critical": ("#fff1f2", "#dc2626"),
            "warning":  ("#fffbeb", "#d97706"),
            "info":     ("#eff6ff", "#3b82f6"),
        }
        _alert_rows_html = ""
        for _al in _alerts[:5]:
            _sev = _al.get("severity", "info")
            _bg, _clr = _sev_cfg.get(_sev, _sev_cfg["info"])
            _title = _al.get("title", "Alert")[:50]
            _alert_rows_html += f"""
<div style="background:{_bg};border-left:3px solid {_clr};
     margin-bottom:4px;border-radius:4px;padding:5px 8px;">
  <span style="font-size:0.75rem;color:#2d3142;font-weight:500;">{_title}</span>
</div>"""
        if not _alert_rows_html:
            _alert_rows_html = '<div style="font-size:0.78rem;color:#64748b;padding:8px;">No active alerts. System healthy.</div>'
        st.markdown(f'<div class="zoho-card">{_alert_rows_html}</div>', unsafe_allow_html=True)

        # Missing inputs notification
        if _missing_count > 0:
            st.markdown(f"""
<div style="background:#fffbeb;border:1px solid #f59e0b;border-radius:8px;padding:8px 12px;margin-top:8px;">
  <span style="font-size:0.78rem;color:#92400e;font-weight:600;">
    {_missing_count} data gaps detected
  </span>
  <span style="font-size:0.72rem;color:#92400e;"> - update for better accuracy</span>
</div>""", unsafe_allow_html=True)
        if st.button("Alert System", key="_home_alert_btn", use_container_width=True):
            st.session_state["_nav_goto"] = "🔔 Alert System"
            st.rerun()

    # ─────────────────────────────────────────────────────────────────────────
    # SECTION 6 — Sales Snapshot + API Health (2-column)
    # ─────────────────────────────────────────────────────────────────────────
    _h6a, _h6b = st.columns(2)

    with _h6a:
        st.markdown('<div class="zoho-row-header">Sales Snapshot (Live Prices)</div>', unsafe_allow_html=True)
        _vg30_mum = _lp.get("DRUM_MUMBAI_VG30", 37000)
        _vg30_kan = _lp.get("DRUM_KANDLA_VG30", 35500)
        _vg10_mum = _lp.get("DRUM_MUMBAI_VG10", 38000)
        _vg10_kan = _lp.get("DRUM_KANDLA_VG10", 36500)
        st.markdown(f"""
<div class="zoho-card">
  <table style="width:100%;border-collapse:collapse;font-size:0.8rem;">
    <thead>
      <tr style="background:#f8fafc;">
        <th style="padding:6px 10px;text-align:left;font-weight:700;color:#1e3a5f;border-bottom:1px solid #e8dcc8;">Grade</th>
        <th style="padding:6px 10px;text-align:right;font-weight:700;color:#1e3a5f;border-bottom:1px solid #e8dcc8;">Mumbai</th>
        <th style="padding:6px 10px;text-align:right;font-weight:700;color:#1e3a5f;border-bottom:1px solid #e8dcc8;">Kandla</th>
      </tr>
    </thead>
    <tbody>
      <tr>
        <td style="padding:5px 10px;color:#2d3142;font-weight:500;">VG-30</td>
        <td style="padding:5px 10px;text-align:right;font-weight:700;color:#2d6a4f;">{_fmt_inr_home(int(_vg30_mum))}</td>
        <td style="padding:5px 10px;text-align:right;font-weight:700;color:#2d6a4f;">{_fmt_inr_home(int(_vg30_kan))}</td>
      </tr>
      <tr style="background:#faf7f2;">
        <td style="padding:5px 10px;color:#2d3142;font-weight:500;">VG-10</td>
        <td style="padding:5px 10px;text-align:right;font-weight:700;color:#2d6a4f;">{_fmt_inr_home(int(_vg10_mum))}</td>
        <td style="padding:5px 10px;text-align:right;font-weight:700;color:#2d6a4f;">{_fmt_inr_home(int(_vg10_kan))}</td>
      </tr>
    </tbody>
  </table>
  <div style="margin-top:10px;display:flex;gap:8px;flex-wrap:wrap;">
    <span class="zoho-stat-pill">{_db_stats.get('total_suppliers', 63)} Suppliers</span>
    <span class="zoho-stat-pill">{_db_stats.get('total_customers', 3)} Customers</span>
    <span class="zoho-stat-pill">{_db_stats.get('total_deals', 0)} Deals</span>
  </div>
</div>
""", unsafe_allow_html=True)

    with _h6b:
        st.markdown('<div class="zoho-row-header">API & System Health</div>', unsafe_allow_html=True)
        _api_display = list(_api_st.items())[:6] if _api_st else []
        _api_rows_html = ""
        for _aname, _adat in _api_display:
            _aok  = _adat.get("status", "") == "OK"
            _dot  = "ok" if _aok else "fail"
            _lat  = _adat.get("avg_latency_ms", 0)
            _label = _aname.replace("_", " ").title()[:22]
            _api_rows_html += f"""
<div class="alert-row" style="justify-content:flex-start;gap:10px;">
  <span class="api-dot {_dot}"></span>
  <span style="font-size:0.75rem;color:#2d3142;flex:1;font-weight:500;">{_label}</span>
  <span style="font-size:0.68rem;color:#64748b;">{_lat}ms</span>
</div>"""
        st.markdown(f"""
<div class="zoho-card">
  <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px;">
    <span style="font-size:0.72rem;color:#64748b;">Live API Status</span>
    <span style="background:#e8f5ee;color:#2d6a4f;font-size:0.68rem;font-weight:700;
                 padding:2px 8px;border-radius:12px;border:1px solid #b7dfc9;">
      {_api_ok}/{_api_tot} Healthy
    </span>
  </div>
  {_api_rows_html}
</div>
""", unsafe_allow_html=True)
        if st.button("API Dashboard", key="_home_api_btn", use_container_width=True):
            st.session_state["_nav_goto"] = "🌐 API Dashboard"
            st.rerun()

    # ─────────────────────────────────────────────────────────────────────────
    # SECTION 7 — AI Command Centre (full width, navy gradient)
    # ─────────────────────────────────────────────────────────────────────────
    st.markdown('<div class="zoho-row-header">AI Command Centre</div>', unsafe_allow_html=True)
    st.markdown("""
<div style="background:linear-gradient(135deg,#1e3a5f 0%,#0f2744 100%);
            border-radius:12px;padding:14px 20px;margin-bottom:12px;">
  <div style="display:flex;gap:8px;align-items:center;margin-bottom:8px;">
    <span style="font-size:0.75rem;font-weight:700;color:#93c5fd;text-transform:uppercase;
                 letter-spacing:0.07em;">PPS Anantam AI — Ask Anything</span>
  </div>
  <div style="color:#e2e8f0;font-size:0.82rem;line-height:1.5;">
    AI assistant connected to live market data, CRM, and all modules.
  </div>
</div>
""", unsafe_allow_html=True)

    _ai_c1, _ai_c2, _ai_c3 = st.columns([3, 3, 4])

    with _ai_c1:
        st.markdown(f"""
<div style="background:rgba(255,255,255,0.05);border:1px solid rgba(201,168,76,0.3);
            border-left:4px solid #c9a84c;border-radius:8px;padding:12px 14px;">
  <div style="color:#fcd34d;font-size:0.65rem;font-weight:700;text-transform:uppercase;
              letter-spacing:0.05em;margin-bottom:6px;">Next Revision</div>
  <div style="font-size:1.3rem;font-weight:800;color:#ffffff;">{_fmt_inr_home(int(_pred))}</div>
  <div style="font-size:0.68rem;color:#93c5fd;margin-top:2px;">/MT VG-30</div>
  <div style="display:flex;gap:10px;margin-top:8px;font-size:0.72rem;">
    <span style="color:#86efac;">{_fmt_inr_home(int(_lo))}</span>
    <span style="color:#fca5a5;">{_fmt_inr_home(int(_hi))}</span>
    <span style="color:#93c5fd;">{_rdate}</span>
  </div>
</div>
""", unsafe_allow_html=True)

    with _ai_c2:
        st.markdown(f"""
<div style="background:rgba(255,255,255,0.05);border:1px solid rgba(255,255,255,0.1);
            border-radius:8px;padding:12px 14px;">
  <div style="color:#93c5fd;font-size:0.65rem;font-weight:700;text-transform:uppercase;
              letter-spacing:0.05em;margin-bottom:8px;">Live Market</div>
  <div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;">
    <div>
      <div style="font-size:0.62rem;color:#94a3b8;">Brent</div>
      <div style="font-size:0.9rem;font-weight:700;color:#e2e8f0;">{mkt['brent']['value']}</div>
    </div>
    <div>
      <div style="font-size:0.62rem;color:#94a3b8;">WTI</div>
      <div style="font-size:0.9rem;font-weight:700;color:#e2e8f0;">{mkt['wti']['value']}</div>
    </div>
    <div>
      <div style="font-size:0.62rem;color:#94a3b8;">USD/INR</div>
      <div style="font-size:0.9rem;font-weight:700;color:#e2e8f0;">{mkt['usdinr']['value']}</div>
    </div>
    <div>
      <div style="font-size:0.62rem;color:#94a3b8;">APIs</div>
      <div style="font-size:0.9rem;font-weight:700;color:#86efac;">{_api_ok}/{_api_tot} OK</div>
    </div>
  </div>
</div>
""", unsafe_allow_html=True)

    with _ai_c3:
        _quick_qs = [
            ("Next predicted price?",        "What is the next predicted bitumen price for the 1st or 16th cycle?"),
            ("News affecting crude?",         "Any breaking news affecting crude oil or bitumen prices today?"),
            ("Highest demand state?",         "Which Indian state has the highest bitumen demand this month?"),
            ("Open bugs & issues?",           "Show me all open bugs and API health issues in the system."),
        ]
        _ql_c1, _ql_c2 = st.columns(2)
        for _i, (_qlabel, _qtext) in enumerate(_quick_qs):
            _col = _ql_c1 if _i % 2 == 0 else _ql_c2
            with _col:
                if st.button(_qlabel, key=f"_home_q_{_i}", use_container_width=True):
                    st.session_state["_ai_prefill"] = _qtext
                    st.session_state["_nav_goto"]   = "🧠 AI Dashboard Assistant"
                    st.rerun()
        if st.button("Open Full AI Chat", key="_home_ai_full",
                     use_container_width=True, type="primary"):
            st.session_state["_nav_goto"] = "🧠 AI Dashboard Assistant"
            st.rerun()

    # ─────────────────────────────────────────────────────────────────────────
    # MISSING INPUTS POPUP (once per day)
    # ─────────────────────────────────────────────────────────────────────────
    try:
        from missing_inputs_engine import MissingInputsEngine as _MIE2
        _mi2 = _MIE2()
        if _mi2.should_show_popup():
            _gaps = _mi2.scan_all_gaps()
            if _gaps:
                with st.expander(f"Data Gaps Detected ({len(_gaps)} items) — Click to review", expanded=False):
                    for _g in _gaps[:8]:
                        _gpri = _g.get("priority", "Medium")
                        _gpri_clr = "#dc2626" if _gpri == "High" else "#d97706" if _gpri == "Medium" else "#3b82f6"
                        st.markdown(f"""
<div style="display:flex;justify-content:space-between;align-items:center;
            padding:4px 0;border-bottom:1px solid #f1f5f9;font-size:0.78rem;">
  <span style="color:#2d3142;">{_g.get('label', _g.get('field', ''))}</span>
  <span style="background:{_gpri_clr};color:#fff;font-size:0.62rem;font-weight:700;
               padding:1px 6px;border-radius:8px;">{_gpri}</span>
</div>""", unsafe_allow_html=True)
    except Exception:
        pass

# ── Handle deferred navigation (from Home quick buttons) ─────────────────────
if st.session_state.get("_nav_goto"):
    _goto = st.session_state.pop("_nav_goto")
    st.session_state['selected_page'] = _goto
    st.rerun()

# Main Layout: 3 Columns (Selection | Analysis | Detailed Slip)
if selected_page == "🧮 Pricing Calculator":
    _render_page_header("🧮 Pricing Calculator", "Sales & Revenue")
    display_badge("calculated")
    col_left, col_mid, col_right = st.columns([1.2, 1.3, 2.0])

    # --- COLUMN 1: SELECTION PANEL & SALES CONTEXT ---
    with col_left:
        st.markdown("### 🔍 Parameters & Context")
        
        # 1. Selection Mode
        search_mode = st.radio("Search By", ["Location", "Customer"], horizontal=True, label_visibility="collapsed")
        
        selected_city = None
        selected_client_name = None 
        
        if search_mode == "Location":
            from distance_matrix import ALL_STATES, get_cities_by_state, get_state_by_city, CITY_STATE_MAP
            
            all_cities = sorted(list(CITY_STATE_MAP.keys()))
            state_options = ["All States"] + ALL_STATES
            selected_state = st.selectbox("📍 Select State", state_options, key="state_select")
            
            if selected_state == "All States":
                city_options = all_cities
            else:
                city_options = sorted(get_cities_by_state(selected_state))
            
            selected_city = st.selectbox("🏙️ Select City", city_options, key="city_select")
            
            if selected_city and selected_state == "All States":
                detected_state = get_state_by_city(selected_city)
                
        else:
            cust_names = sorted(list(customer_city_map.keys()))
            selected_cust = st.selectbox("Select Customer", cust_names)
            if selected_cust:
                selected_city = customer_city_map[selected_cust]
                selected_client_name = selected_cust
                from distance_matrix import get_state_by_city
                cust_state = get_state_by_city(selected_city)
                st.info(f"📍 {selected_city}, {cust_state}")

        # --- SALES CONTEXT WIDGET (NEW) ---
        if selected_city:
            today = datetime.date.today()
            # Determine state
            if search_mode == "Location" and 'selected_state' in locals() and selected_state != "All States":
                ctx_state = selected_state
            else:
                from distance_matrix import get_state_by_city
                ctx_state = get_state_by_city(selected_city)

            # Get Data
            season_info = get_season_status(selected_city, today.month)
            holidays = get_holidays_for_month(today.year, today.month, ctx_state)
            
            # --- SALES CARD ---
            st.markdown(f"""
<div style="border: 1px solid #ddd; border-radius: 8px; padding: 10px; background-color: #fff; margin: 10px 0; box-shadow: 0 1px 3px rgba(0,0,0,0.05);">
<div style="font-weight: bold; color: #444; margin-bottom: 5px; border-bottom: 1px solid #eee; padding-bottom: 3px;">
🗣️ Sales Insights for {selected_city}
</div>
<div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px;">
<span style="font-size: 0.85em; color: #666;">Season:</span>
<span style="background-color: {season_info['color']}; color: white; padding: 2px 8px; border-radius: 10px; font-size: 0.8em; font-weight: bold;">
{season_info['label'].replace('✅ ', '').replace('❌ ', '').replace('⚠️ ', '')}
</span>
</div>
""", unsafe_allow_html=True)

            # Holidays Integration
            if holidays:
                next_holiday = holidays[0]
                days_left = next_holiday['date'].day - today.day
                holiday_msg = ""
                if days_left == 0:
                     holiday_msg = f"🎉 Today is <b>{next_holiday['name']}</b>!"
                elif 0 < days_left <= 7:
                     holiday_msg = f"📅 Upcoming: <b>{next_holiday['name']}</b> in {days_left} days."
                else:
                     holiday_msg = f"📅 {next_holiday['date'].day} {MONTH_NAMES[today.month]}: {next_holiday['name']}"
                
                if holiday_msg:
                    st.markdown(f"""
<div style="background-color: #fce4ec; color: #c2185b; padding: 6px; border-radius: 4px; font-size: 0.8em; margin-bottom: 5px;">
{holiday_msg}
</div>
""", unsafe_allow_html=True)
            else:
                st.caption("No major holidays this month.")
            
            # Remarks (Weather)
            remarks = get_city_remarks(selected_city)
            if remarks:
                 st.caption(f"📝 {remarks}")
                 
            st.markdown("</div>", unsafe_allow_html=True)
            # ---------------------------

        # 2. Product Info
        product_grade = st.radio("Select Grade", ["VG30", "VG10"], horizontal=True)
        
        load_type = "Bulk" 
        if search_mode == "Location": 
             load_type = st.radio("Select Type", ["Bulk", "Drum"], horizontal=True)
             
        product_name = f"Bitumen {product_grade}"
        
        calc_btn = st.button("Calculate Cost", use_container_width=True, type="primary")

    # --- COLUMN 2: RANKING LIST (Landing Price) - CATEGORIZED ---
    with col_mid:
        st.markdown("### 📉 Best Prices")
        
        # Initialize result to None
        result = None
        selected_option = None

        if selected_city and calc_btn:
            # Get feasibility assessment for this destination
            assessment = get_feasibility_assessment(selected_city, top_n=3, grade=product_grade)
            
            if assessment:
                st.caption(f"Ranking for {product_grade} | Destination: {selected_city}")
                
                # Build options list for selection
                price_options = []
                
                # --- 1. DRUM DIRECT (TOP PRIORITY) ---
                st.markdown("#### 🛢️ Drum Import (Main)")
                drum = assessment.get('drum_direct')
                if drum:
                    price_options.append({
                        'label': f"🛢️ {drum['source']} - {format_inr(drum['landed_cost'])}",
                        'source': drum['source'],
                        'price': drum['landed_cost'],
                        'base': drum.get('base_price', 0),
                        'transport': drum.get('transport', 0),
                        'distance': drum.get('distance_km', 0),
                        'type': 'Drum'
                    })
                    st.markdown(f'''
<div style="background-color:#FADBD8; padding:10px; border-radius:5px; margin-bottom:5px; border-left:5px solid #C0392B;">
<b style="font-size:1.1em;">{drum['source']}</b><br>
<span style="font-weight:bold; font-size:1.3em; color:#C0392B;">🛢️ {format_inr(drum['landed_cost'])} PMT</span>
<small style="color:#666;"> ({drum['distance_km']:.0f} km)</small>
</div>
''', unsafe_allow_html=True)
                
                # --- 2. LOCAL DECANTER BULK ---
                st.markdown("#### 🔄 Decanter Bulk")
                dec = assessment.get('local_decanter')
                if dec:
                    price_options.append({
                        'label': f"🔄 {dec['source']} - {format_inr(dec['landed_cost'])}",
                        'source': dec['source'],
                        'price': dec['landed_cost'],
                        'base': dec.get('drum_base_price', 0),
                        'transport': dec.get('drum_transport', 0) + dec.get('local_transport', 0),
                        'distance': 30,
                        'type': 'Decanter Bulk'
                    })
                    st.markdown(f'''
<div style="background-color:#FCF3CF; padding:8px; border-radius:5px; margin-bottom:5px; border-left:5px solid #9A7D0A;">
<small><b>{dec['source']}</b></small><br>
<small>From: {dec.get('drum_source', 'N/A')} | Conv: {dec.get('conversion_cost', 500)}</small><br>
<span style="font-weight:bold; font-size:1.1em; color:#9A7D0A;">🟢 {format_inr(dec['landed_cost'])} PMT</span>
</div>
''', unsafe_allow_html=True)
                
                # --- 3. IMPORT BULK ---
                st.markdown("#### 🚢 Import Bulk")
                for i, opt in enumerate(assessment['imports'][:2]):
                    price_options.append({
                        'label': f"🚢 {opt['source']} - {format_inr(opt['landed_cost'])}",
                        'source': opt['source'],
                        'price': opt['landed_cost'],
                        'base': opt.get('base_price', 0),
                        'transport': opt.get('transport', 0),
                        'distance': opt.get('distance_km', 0),
                        'type': 'Import Bulk'
                    })
                    bg_color = "#D6EAF8" if i == 0 else "#EBF5FB"
                    border = "#21618C" if i == 0 else "#AED6F1"
                    st.markdown(f'''
<div style="background-color:{bg_color}; padding:8px; border-radius:5px; margin-bottom:5px; border-left:5px solid {border};">
<small><b>{opt['source']}</b></small><br>
<span style="font-weight:bold; font-size:1.1em; color:{border};">{format_inr(opt['landed_cost'])} PMT</span>
<small style="color:#666;"> ({opt['distance_km']:.0f} km)</small>
</div>
''', unsafe_allow_html=True)
                
                # --- 4. PSU REFINERIES (Last) ---
                st.markdown("#### 🏭 PSU Refinery Bulk")
                for i, opt in enumerate(assessment['refineries'][:2]):
                    price_options.append({
                        'label': f"🏭 {opt['source']} - {format_inr(opt['landed_cost'])}",
                        'source': opt['source'],
                        'price': opt['landed_cost'],
                        'base': opt.get('base_price', 0),
                        'transport': opt.get('transport', 0),
                        'distance': opt.get('distance_km', 0),
                        'type': 'Refinery Bulk'
                    })
                    bg_color = "#D4EFDF" if i == 0 else "#EAFAF1"
                    border = "#196F3D" if i == 0 else "#A9DFBF"
                    st.markdown(f'''
<div style="background-color:{bg_color}; padding:8px; border-radius:5px; margin-bottom:5px; border-left:5px solid {border};">
<small><b>{opt['source']}</b></small><br>
<span style="font-weight:bold; font-size:1.1em; color:{border};">{format_inr(opt['landed_cost'])} PMT</span>
<small style="color:#666;"> ({opt['distance_km']:.0f} km)</small>
</div>
''', unsafe_allow_html=True)
                
                # --- SELECT PRICE FOR PDF ---
                st.markdown("---")
                st.markdown("**📋 Select Price for Quote:**")
                option_labels = [opt['label'] for opt in price_options]
                selected_label = st.radio("Choose option for PDF", option_labels, key="pdf_price_select", label_visibility="collapsed")
                
                # Find selected option
                for opt in price_options:
                    if opt['label'] == selected_label:
                        selected_option = opt
                        break
                
                # Store in session state for PDF
                if selected_option:
                    st.session_state['selected_price_option'] = selected_option
                    st.success(f"✅ Selected: {selected_option['source']} @ {format_inr(selected_option['price'])}")
                
                # Set result for the right panel
                result = {
                    'best_option': {
                        'source_location': selected_option['source'] if selected_option else 'N/A',
                        'final_landed_cost': selected_option['price'] if selected_option else 0,
                        'base_price': selected_option['base'] if selected_option else 0,
                        'transport_cost': selected_option['transport'] if selected_option else 0,
                        'discount': 0,
                        'distance_km': selected_option['distance'] if selected_option else 0,
                        'type': selected_option['type'] if selected_option else 'N/A'
                    },
                    'all_options': None,
                    'assessment': assessment
                }
            else:
                # Fallback to original optimizer
                result = optimizer.calculate_best_price(selected_city, load_type, product_grade=product_grade)
                
                if result:
                     st.caption(f"Ranking for {load_type} | {product_grade}")
                     top_sources = result['all_options'].head(5)
                     for idx, (i, row) in enumerate(top_sources.iterrows()):
                        icon = "🟢" if idx == 0 else "🔴"
                        text_color = "#27AE60" if idx == 0 else "#C0392B"
                        bg_color = "#EAFAF1" if idx == 0 else "#FADBD8"
                        
                        st.markdown(f"""
<div style="background-color:{bg_color}; padding:8px; border-radius:5px; margin-bottom:5px; border-left: 5px solid {text_color};">
<small><b>{row['source_location']}</b></small><br>
<span style="color:{text_color}; font-weight:bold; font-size:1.1em;">{icon} {format_inr(row['final_price'])} PMT</span>
</div>
""", unsafe_allow_html=True)
                        
            if not result:
                st.warning("No Data Found.")
                

        elif selected_city:
            st.info("Press 'Calculate Cost' to see pricing.")




    # --- COLUMN 3: DETAILED SLIP (Landing Cost Information) ---
    with col_right:
        st.markdown("### 📝 Landing Cost Information")
        
        # Display details only if a calculation has been performed and result is available
        if selected_city and calc_btn and 'result' in locals() and result:
            # Get best option from result
            best_opt = result.get('best_option', {})
            assessment = result.get('assessment', {})
            
            # Header Box
            st.markdown(f"""
<div style="background-color:#F5B041; padding:5px; text-align:center; font-weight:bold;">
{load_type.upper()} ENQUIRY DETAILS (2-2-2026)
</div>
""", unsafe_allow_html=True)
            
            # Details Table Logic using Columns
            d1, d2 = st.columns([2, 1])
            
            with d1:
                source_name = best_opt.get('source_location', 'N/A')
                st.write(f"**From**: {source_name}")
                st.write(f"**To**: {selected_city}")
                st.write(f"**Product**: {product_name}")
                st.divider()
                
                # Maths
                base_price = best_opt.get('base_price', 0)
                discount = best_opt.get('discount', 0)
                transport = best_opt.get('transport_cost', 0)
                final_cost = best_opt.get('final_landed_cost', 0)
                distance = best_opt.get('distance_km', 0)
                
                st.write(f"Basic Rate: {format_inr(base_price)} PMT")
                st.write(f"Discount: - {format_inr(discount)}")
                
                tax_val = base_price * 0.18
                st.write(f"GST 18%: + {format_inr(tax_val)}")
                
                net_start = base_price + tax_val - discount
                st.markdown(f"**Ex-Refinery Price: {format_inr(net_start)} PMT**")
                
                st.info(f"➕ Transport: {format_inr(transport)}")
                
                # Show Mileage if available
                if distance > 0:
                    rate_km = transport / distance if distance > 0 else 0
                    st.caption(f"📏 Distance: {distance:.0f} KM | Rate: ₹ {rate_km:.2f}/KM")
            
                st.success(f"**FINAL LANDING COST: {format_inr(final_cost)} PMT**")

            with d2:
                st.markdown("**Terms & Conditions**")
                st.caption("1. Price valid for 24 hours only.")
                st.caption("2. Payment: 100% Advance before dispatch.")
                st.caption("3. We do NOT arrange, pay, or refer any transporter.")
                st.caption("4. Buyer is responsible for all transport payment & issues.")
                st.caption("5. Driver/Transporter is final authority to accept/reject qty, quality, or packing.")
                st.caption("6. All halting & demurrage charges at loading point - Driver's responsibility.")
                st.caption("7. All loading & unloading charges - at Driver's cost.")
                st.caption("8. Subject to material availability.")
                
                st.markdown("---")
                st.markdown("🏦 **Pay To:**")
                st.caption(f"ICICI Bank | A/C: 184105001402")
                st.caption(f"IFSC: ICIC0001841")
            

            # PDF GENERATION (Legacy Wrapper)
            # We keep the legacy function for now but add a generic call to the new system if needed.
            # For this interaction, we will stick to the existing visual flow but using the new robust engine is possible.
            
            # --- NEW: GENERATE FORMAL QUOTE (Via new System) ---
            if st.button("🚀 Generate & Save Formal Quote (SQL DB)"):
                try:
                    from quotation_system.models import Quotation, QuotationItem
                    from quotation_system.pdf_maker import generate_pdf
                    from quotation_system.db import create_db_and_tables, engine
                    from sqlmodel import Session
                    import datetime
                    
                    # Ensure DB exists
                    create_db_and_tables()
                    
                    # Create Session
                    with Session(engine) as session:
                        # Prepare Data
                        q_num = get_next_quote_number() # Use existing counter for ID
                        
                        # Calc Totals
                        row_price = final_cost  # Use the variable from above
                        qty = 1.0
                        basic = qty * row_price
                        tax = basic * 0.18
                        grand = basic + tax
                        
                        # Create Object
                        new_quote = Quotation(
                            quote_number=q_num,
                            quote_date=datetime.date.today(),
                            valid_until=datetime.date.today() + datetime.timedelta(days=1),
                            seller_name="PPS Anantams Corporation Pvt. Ltd.",
                            seller_address="04, Signet plaza Tower- B, Vadodara-390021",
                            seller_gstin="24AAHCV1611L2ZD",
                            buyer_name=selected_client_name or "Valued Client",
                            buyer_address="Billing Address...",
                            delivery_terms=f"FOR {selected_city}",
                            payment_terms="100% Advance",
                            dispatch_mode="Road",
                            subtotal=basic,
                            total_tax=tax,
                            grand_total=grand,
                            status="FINAL"
                        )
                        
                        # Add Item
                        item = QuotationItem(
                            product_name=f"{product_name} ({load_type})",
                            description=f"Source: {source_name}",
                            quantity=qty,
                            rate=row_price,
                            total_amount=basic
                        )
                        new_quote.items = [item]
                        
                        session.add(new_quote)
                        session.commit()
                        session.refresh(new_quote)
                        
                        # Generate PDF
                        pdf_path = f"Formal_Quote_{new_quote.quote_number.replace('/','-')}.pdf"
                        generate_pdf(new_quote, pdf_path)
                        
                        st.success(f"✅ Quote Saved to DB (ID: {new_quote.id}) and PDF Generated!")
                        
                        # Download Button
                        with open(pdf_path, "rb") as f:
                            st.download_button("📄 Download Formal PDF", f, file_name=pdf_path)
                            
                except Exception as e:
                    st.error(f"System Error: {e}")

            # Legacy PDF Logic (Kept for fallback)
            pdf_filename = f"Quote_{selected_city}_{product_grade}.pdf"
            client_name_for_pdf = selected_client_name if 'selected_client_name' in locals() and selected_client_name else "Valued Customer"
            
            # Generate Quote Number (Simulating a fresh quote on every calculation)
            # In a real app, we might want to persist this per session until reset
            from pdf_generator import get_next_quote_number
            if 'current_quote_no' not in st.session_state:
                st.session_state.current_quote_no = get_next_quote_number()
                
            # If user clicked calculate again, maybe generate new one? 
            # For now, let's keep one number per session or button press. 
            # The 'calc_btn' trigger implies a fresh calc.
            
            # Actually, to increase on EVERY new calculation:
            # We should probably do it inside the 'if calc_btn' block, 
            # BUT streamlit reruns the script, so 'if calc_btn' is true on that run.
            # We'll just fetch a new one if it's not set or reuse. 
            # To strictly follow "auto increase from 1 on every new quote", we need to know when it is "new".
            # Let's assume hitting "Calculate" = New Quote.
            
            # Use the number stored in session state if available, or generate?
            # Issue: Streamlit reruns. If I put get_next() here, it runs on every interaction.
            # Fix: Only run if we just entered this block. 
            # Simplest: Just call it. If the number jumps too fast, we'll refine.
            # But wait, user wants it to be sequential.
            # I will use a session state flag to ensure I don't increment on simple UI refreshes like toggling tabs.
            
            # For this Step, let's just generate it fresh to satisfy the requirement
            quote_no = get_next_quote_number() # Warning: Might skip numbers on refresh
            
            # Generate the file
            create_price_pdf(client_name_for_pdf, product_name, source_name, final_cost, filename=pdf_filename, quote_no=quote_no)
            
            # Read Bytes for Download
            with open(pdf_filename, "rb") as f:
                st.download_button(
                    label="📄 Download Official Quote PDF",
                    data=f,
                    file_name=pdf_filename,
                    mime="application/pdf",
                    use_container_width=True
                )
            
            # WHATSAPP INTEGRATION
            wa_message = f"""*PPS Anantams Price Offer* 🚛
Product: {product_name}
Best Source: {source_name}
Est. Distance: {distance:.0f} KM

*Final Landed Rate: {format_inr(final_cost)} PMT*

_Terms: 100% Advance. Valid for 24 Hrs._
"""
            encoded_wa = urllib.parse.quote(wa_message)
            wa_link = f"https://wa.me/?text={encoded_wa}"
            
            st.markdown(f"""
<a href="{wa_link}" target="_blank" style="text-decoration:none;">
<button style="width:100%; border:none; background-color:#25D366; color:white; padding:10px; border-radius:5px; margin-top:5px; font-weight:bold; cursor:pointer;">
💬 Share via WhatsApp
</button>
</a>
""", unsafe_allow_html=True)
        elif selected_city:
            st.info("Select configuration and click 'Calculate Cost' to see detailed information.")
        else:
            st.info("Please select a city to begin.")

# --- TAB: SALES WORKSPACE (NEW) ---
if selected_page == "💼 Sales Workspace":
    _render_page_header("💼 Sales Workspace", "Sales & Revenue")
    display_badge("historical")
    sales_workspace.render_deal_room()

# --- TAB 2: SALES CALENDAR ---
if selected_page == "📅 Sales Calendar":
    _render_page_header("📅 Sales Calendar", "Sales & Revenue")
    st.header("📅 Sales Calendar - Season & Holiday Planner")
    st.caption("Understand peak/off seasons, holidays, and best times to contact clients by city")
    
    # Import calendar functions
    from sales_calendar import (
        CITY_SEASONS, get_season_status, get_holidays_for_month, 
        get_sundays, get_yearly_overview, get_city_remarks,
        MONTH_NAMES, STATE_HOLIDAYS, MAJOR_FESTIVALS_2026
    )
    from distance_matrix import CITY_STATE_MAP, ALL_STATES, get_cities_by_state
    import calendar
    
    # City/State Selection
    cal_col1, cal_col2, cal_col3 = st.columns([1, 1, 2])
    
    with cal_col1:
        cal_state = st.selectbox("📍 Select State", ["All States"] + ALL_STATES, key="cal_state")
        if cal_state == "All States":
            cal_cities = sorted(list(CITY_STATE_MAP.keys()))
        else:
            cal_cities = sorted(get_cities_by_state(cal_state))
        cal_city = st.selectbox("🏙️ Select City", cal_cities, key="cal_city")
    
    with cal_col2:
        # Get city state for holidays
        city_state = CITY_STATE_MAP.get(cal_city, "Gujarat")
        st.info(f"**State:** {city_state}")
        
        # Current month indicator
        today = datetime.date.today()
        current_season = get_season_status(cal_city, today.month)
        
        st.markdown(f"""
<div style="background-color:{current_season['color']}; color:white; padding:15px; border-radius:10px; text-align:center;">
<b style="font-size:1.3em;">Current Status</b><br>
<span style="font-size:1.5em;">{current_season['label']}</span>
</div>
""", unsafe_allow_html=True)
    
    with cal_col3:
        remarks = get_city_remarks(cal_city)
        if remarks:
            st.warning(f"📋 **Weather Note:** {remarks}")
    
    st.markdown("---")
    
    # YEARLY OVERVIEW - 12 Month Grid
    st.subheader(f"📊 Year 2026 - Season Overview for {cal_city}")
    
    yearly = get_yearly_overview(cal_city)
    
    # Create 12-month grid (4 columns x 3 rows)
    row1 = st.columns(4)
    row2 = st.columns(4)
    row3 = st.columns(4)
    
    all_rows = [row1, row2, row3]
    month_idx = 0
    
    for row in all_rows:
        for col in row:
            if month_idx < 12:
                m_data = yearly[month_idx]
                with col:
                    st.markdown(f"""
<div style="background-color:{m_data['color']}; color:white; padding:10px; border-radius:8px; text-align:center; margin:2px;">
<b>{m_data['month_name']}</b><br>
<small>{m_data['label'].replace('✅ ', '').replace('❌ ', '').replace('⚠️ ', '')}</small>
</div>
""", unsafe_allow_html=True)
                month_idx += 1
    
    st.markdown("---")
    
    # MONTHLY CALENDAR VIEW
    st.subheader("📆 Monthly Calendar View")
    
    month_cols = st.columns([1, 2, 1])
    with month_cols[1]:
        selected_month = st.selectbox("Select Month", 
            options=list(range(1, 13)),
            format_func=lambda x: MONTH_NAMES[x],
            index=today.month - 1,
            key="cal_month"
        )
    
    year = 2026
    
    # Get calendar data
    cal_data = calendar.monthcalendar(year, selected_month)
    sundays = get_sundays(year, selected_month)
    holidays = get_holidays_for_month(year, selected_month, city_state)
    month_season = get_season_status(cal_city, selected_month)
    
    # Season banner
    st.markdown(f"""
<div style="background-color:{month_season['color']}; color:white; padding:10px; border-radius:8px; text-align:center; margin-bottom:10px;">
<b>{MONTH_NAMES[selected_month]} 2026</b> - {month_season['label']}
</div>
""", unsafe_allow_html=True)
    
    # Calendar grid with days
    st.markdown("##### 📅 Calendar")
    
    # Header row
    day_names = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    header_cols = st.columns(7)
    for i, day_name in enumerate(day_names):
        bg = "#E74C3C" if day_name == "Sun" else "#2C3E50"
        header_cols[i].markdown(f"""
<div style="background-color:{bg}; color:white; padding:5px; text-align:center; border-radius:5px;">
<b>{day_name}</b>
</div>
        """, unsafe_allow_html=True)
    
    # Calendar weeks
    holiday_days = {h['date'].day: h['name'] for h in holidays}
    
    for week in cal_data:
        week_cols = st.columns(7)
        for i, day in enumerate(week):
            with week_cols[i]:
                if day == 0:
                    st.write("")
                else:
                    # Determine day color
                    is_sunday = (i == 6)
                    is_holiday = day in holiday_days
                    
                    if is_sunday:
                        bg = "#E74C3C"
                        txt = "white"
                    elif is_holiday:
                        bg = "#9B59B6"
                        txt = "white"
                    else:
                        bg = "#ECF0F1"
                        txt = "#2C3E50"
                    
                    holiday_indicator = "🎉" if is_holiday else ""
                    
                    st.markdown(f"""
<div style="background-color:{bg}; color:{txt}; padding:8px; text-align:center; border-radius:5px; margin:1px; min-height:40px;">
<b>{day}</b> {holiday_indicator}
</div>
""", unsafe_allow_html=True)
    
    # Holidays List
    st.markdown("---")
    st.subheader(f"🎊 Holidays & Festivals in {MONTH_NAMES[selected_month]}")
    
    if holidays:
        for h in holidays:
            type_color = "#E74C3C" if h['type'] == 'national' else "#9B59B6" if h['type'] == 'festival' else "#3498DB"
            type_label = "🇮🇳 National" if h['type'] == 'national' else "🎉 Festival" if h['type'] == 'festival' else "🏛️ State"
            st.markdown(f"""
<div style="background-color:#FFF; border-left:4px solid {type_color}; padding:8px; margin:5px 0; border-radius:3px;">
<b>{h['date'].day} {MONTH_NAMES[selected_month]}</b> - {h['name']} <small>({type_label})</small>
</div>
""", unsafe_allow_html=True)
    else:
        st.info("No major holidays this month.")
    
    # UPCOMING IMPORTANT DATES
    st.markdown("---")
    st.subheader("📌 Upcoming Important Dates (Next 30 Days)")
    
    upcoming = []
    for m, d, name, duration in MAJOR_FESTIVALS_2026:
        festival_date = datetime.date(2026, m, d)
        days_until = (festival_date - today).days
        if 0 <= days_until <= 30:
            upcoming.append((days_until, f"{d} {MONTH_NAMES[m]}", name))
    
    if upcoming:
        for days, date_str, name in sorted(upcoming):
            urgency = "🔴" if days <= 7 else "🟡" if days <= 14 else "🟢"
            st.markdown(f"{urgency} **{date_str}** - {name} ({days} days away)")
    else:
        st.info("No major festivals in next 30 days.")
    
    # SALES TIPS
    st.markdown("---")
    st.subheader(f"💡 Sales Tips for {cal_city}")
    
    tips_col1, tips_col2 = st.columns(2)
    
    with tips_col1:
        st.markdown("**🎯 Best Time to Call:**")
        if month_season['status'] == 'peak':
            st.success("✅ NOW is ideal! High demand season.")
            st.caption("• Contractors actively placing orders")
            st.caption("• Follow up on pending quotes")
            st.caption("• Push for bulk orders")
        elif month_season['status'] == 'off':
            st.error("❌ Low demand period")
            st.caption("• Focus on relationship building")
            st.caption("• Discuss advance bookings for next season")
            st.caption("• Collect payments from previous orders")
        else:
            st.warning("⚠️ Moderate season")
            st.caption("• Good for quote discussions")
            st.caption("• Push urgent project orders")
    
    with tips_col2:
        st.markdown("**🗣️ Conversation Topics:**")
        # Check for upcoming festivals
        next_festival = None
        for m, d, name, duration in MAJOR_FESTIVALS_2026:
            fest_date = datetime.date(2026, m, d)
            if 0 < (fest_date - today).days <= 30:
                next_festival = name
                break
        
        if next_festival:
            st.info(f"🎉 Wish client for: **{next_festival}**")
        
        if city_state in STATE_HOLIDAYS and "holidays" in STATE_HOLIDAYS[city_state]:
            local_festivals = [f[2] for f in STATE_HOLIDAYS[city_state]["holidays"]]
            if local_festivals:
                st.caption(f"Local festivals to mention: {', '.join(local_festivals[:3])}")
        
        st.caption("• Ask about ongoing projects")
        st.caption("• Enquire about tender participation")
        st.caption("• Discuss competitor pricing if known")

# --- TAB 3: PARTY MANAGEMENT ---
# --- TAB 3: PARTY MANAGEMENT ---
if selected_page == "👥 Ecosystem Management":
    _render_page_header("👥 Ecosystem Management", "Technology")
    st.header("👥 Ecosystem Management")
    st.caption("Manage Suppliers, Customers, and Logistics Partners in your ecosystem.")
    
    # Import party functions
    from party_master import (
        load_suppliers, save_suppliers, add_supplier,
        load_customers, save_customers, add_customer,
        load_services, save_services, add_service_provider,
        import_sales_from_excel, toggle_purchase_party,
        SUPPLIER_CATEGORIES, CUSTOMER_CATEGORIES, SERVICE_CATEGORIES
    )
    
    # --- ANALYTICS DASHBOARD (NEW) ---
    with st.expander("📊 Ecosystem Analytics (Head Count Summary)", expanded=True):
        st.caption("Total counts of Suppliers, Service Providers, and Customers by Category, City, and State.")
        
        # Load all data
        all_suppliers = load_suppliers()
        all_services = load_services()
        all_customers = load_customers()
        
        # Helper to get state
        from distance_matrix import get_state_by_city
        
        # Create Columns for High Level Stats
        stat1, stat2, stat3, stat4 = st.columns(4)
        total_partners = len(all_suppliers) + len(all_services) + len(all_customers)
        stat1.metric("🌍 Total Ecosystem", total_partners)
        stat2.metric("🏭 Suppliers", len(all_suppliers))
        stat3.metric("🚚 Logistics", len(all_services))
        stat4.metric("💰 Customers", len(all_customers))
        
        st.markdown("---")
        
        # Prepare Data for Aggregation
        # Combine Suppliers + Services for "Partner Ecosystem" analysis
        ecosystem_data = []
        
        def get_clean_state(raw_city):
            if not raw_city: return "Unknown"
            # Cleaning city name (e.g. "Mumbai (Chembur)" -> "Mumbai")
            clean_city = raw_city.split('(')[0].strip()
            
            # Manual Mapping for specific cases
            if 'Navi Mumbai' in raw_city: return 'Maharashtra'
            if 'Chembur' in raw_city: return 'Maharashtra'
            if 'Taloja' in raw_city: return 'Maharashtra'
            if 'Kandla' in raw_city or 'Mundra' in raw_city: return 'Gujarat'
            if 'Haldia' in raw_city: return 'West Bengal'
            if 'Bhatinda' in raw_city: return 'Punjab'
            
            # Standard Lookup
            state = get_state_by_city(clean_city)
            
            # Fallback for capitals if needed
            if state == "Unknown":
                if clean_city in ['Delhi', 'New Delhi']: return 'Delhi'
                if clean_city == 'Guwahati': return 'Assam'
                if clean_city == 'Bhubaneswar': return 'Odisha'
                if clean_city == 'Kolkata': return 'West Bengal'
            
            return state

        for s in all_suppliers:
            city_val = s.get('city', 'Unknown')
            ecosystem_data.append({
                'Name': s.get('name'),
                'Category': s.get('type'),
                'City': city_val,
                'State': get_clean_state(city_val),
                'Group': 'Supplier'
            })
            
        for s in all_services:
            city_val = s.get('city', 'Unknown')
            ecosystem_data.append({
                'Name': s.get('name'),
                'Category': s.get('category'),
                'City': city_val,
                'State': get_clean_state(city_val),
                'Group': 'Logistics'
            })
            
        for c in all_customers:
             city_val = c.get('city', 'Unknown')
             ecosystem_data.append({
                'Name': c.get('name'),
                'Category': c.get('category', 'General'), # Use category, fallback to General
                'City': city_val,
                'State': get_clean_state(city_val),
                'Group': 'Customer'
            })

        if ecosystem_data:
            df_eco = pd.DataFrame(ecosystem_data)
            
            # TABS for specific breakdowns
            ana_tab1, ana_tab2, ana_tab3 = st.tabs(["📂 By Category", "🏙️ By City", "🗺️ By State"])
            
            with ana_tab1:
                st.subheader("Head Count by Category")
                cat_counts = df_eco['Category'].value_counts().reset_index()
                cat_counts.columns = ['Category', 'Count']
                st.dataframe(cat_counts, use_container_width=True, hide_index=True)
                
                # Bar Chart
                st.bar_chart(cat_counts.set_index('Category'))

            with ana_tab2:
                st.subheader("Head Count by City")
                city_counts = df_eco['City'].value_counts().reset_index()
                city_counts.columns = ['City', 'Count']
                st.dataframe(city_counts, use_container_width=True, hide_index=True)

            with ana_tab3:
                st.subheader("Head Count by State")
                state_counts = df_eco['State'].value_counts().reset_index()
                state_counts.columns = ['State', 'Count']
                st.dataframe(state_counts, use_container_width=True, hide_index=True)
                
                # Show Map visualization if possible (simple scatter map requires lat/lon, skipping for now to keep it simple stats)
                
        else:
            st.info("No ecosystem data available to analyze.")

    # Sub-tabs
    m_tab1, m_tab2, m_tab3 = st.tabs(["🏭 Source & Supply", "💰 Sales & Clients", "🚚 Logistics & Services"])
    
    # ======== 1. SOURCE & SUPPLY ========
    with m_tab1:
        st.subheader("Manage Sources (Exporters, Importers, Mfg)")
        
        # Display Section
        suppliers = load_suppliers()
        if suppliers:
            # Convert to DF for easy viewing
            s_df = pd.DataFrame(suppliers)
            
            # --- FILTERS & SORTING ---
            c_f1, c_f2, c_f3 = st.columns([2, 1, 1])
            with c_f1:
                # Filter by Category
                cat_filter = st.multiselect("Filter Category", SUPPLIER_CATEGORIES, default=["Importer of India"])
            with c_f2:
                # Filter by City
                unique_cities = sorted(list(set([s.get('city', '') for s in suppliers if s.get('city')])))
                city_filter = st.multiselect("Filter by City", unique_cities)
            with c_f3:
                # Sorting
                sort_order = st.radio("Sort Name", ["A-Z", "Z-A"], horizontal=True)
            
            # Apply Filters
            if not cat_filter:
                cat_filter = SUPPLIER_CATEGORIES # Show all if none selected
                
            filtered_s = [s for s in suppliers if s.get('type') in cat_filter or s.get('type') in ['Bulk', 'Bulk/PSU', 'Drum']]
            
            if city_filter:
                filtered_s = [s for s in filtered_s if s.get('city') in city_filter]
                
            # Apply Sorting
            reverse_sort = True if sort_order == "Z-A" else False
            filtered_s = sorted(filtered_s, key=lambda x: x.get('name', '').lower(), reverse=reverse_sort)
            
            if filtered_s:
                s_df = pd.DataFrame(filtered_s)
                
                # Standardize Columns
                required_cols = ['name', 'type', 'city', 'contact', 'details', 'gstin']
                for col in required_cols:
                    if col not in s_df.columns:
                        s_df[col] = "" # Fill missing cols
                
                # Select & Rename for Display
                display_df = s_df[required_cols].copy()
                display_df.columns = ["Company Name", "Category", "City", "Contact / Email", "Address / Details", "GSTIN"]
                
                st.dataframe(display_df, use_container_width=True, hide_index=True)
            else:
                st.info("No suppliers found in selected categories.")
        
        st.markdown("---")
        st.markdown("#### ➕ Add New Source")
        with st.form("add_supplier_form"):
            c1, c2 = st.columns(2)
            with c1:
                s_name = st.text_input("Company Name")
                s_cat = st.selectbox("Category", SUPPLIER_CATEGORIES)
                s_city = st.text_input("City/Location")
            with c2:
                s_contact = st.text_input("Contact Number/Person")
                s_gst = st.text_input("GSTIN")
                s_details = st.text_area("Extra Details (Loading Person/Notes)", height=40)
            
            if st.form_submit_button("Add Source"):
                if s_name and s_cat:
                    add_supplier(s_name, s_cat, s_city, s_contact, s_gst, s_details)
                    st.success(f"Added {s_name} to database!")
                    st.rerun()
                else:
                    st.error("Name and Category are required.")

    # ======== 2. SALES & CLIENTS ========
    with m_tab2:
        st.subheader("Manage Customers (Contractors & Traders)")
        
        customers = load_customers()
        
        # Stats
        if customers:
            total_c = len(customers)
             # View Data
            st.markdown("#### 📋 Client Directory")
            
            # --- FILTERS & SORTING ---
            cf_1, cf_2, cf_3 = st.columns([2, 1, 1])
            with cf_1:
                c_filter = st.selectbox("Filter Category", ["All"] + CUSTOMER_CATEGORIES)
            with cf_2:
                # Get unique states from customers
                cust_states = sorted(list(set([c.get('state', '') for c in customers if c.get('state')])))
                s_filter = st.multiselect("Filter State", cust_states)
            with cf_3:
                cust_sort = st.radio("Sort", ["A-Z", "Z-A"], horizontal=True, key="cust_sort")
            
            filtered_c = customers
            if c_filter != "All":
                filtered_c = [c for c in filtered_c if c.get('category') == c_filter]
                
            if s_filter:
                filtered_c = [c for c in filtered_c if c.get('state') in s_filter]
            
            # Apply Sorting
            rev_cust = True if cust_sort == "Z-A" else False
            filtered_c = sorted(filtered_c, key=lambda x: x.get('name', '').lower(), reverse=rev_cust)

            if filtered_c:
                c_df = pd.DataFrame(filtered_c)
                # Standardize
                req_cols_c = ['name', 'category', 'city', 'state', 'contact', 'gstin']
                for col in req_cols_c:
                    if col not in c_df.columns:
                        c_df[col] = ""
                        
                display_c = c_df[req_cols_c].copy()
                display_c.columns = ["Client Name", "Segment", "City", "State", "Contact / Mobile", "GSTIN"]
                st.dataframe(display_c, use_container_width=True, hide_index=True)
            else:
                st.info("No clients found matching criteria.")
        
        st.markdown("---")
        st.markdown("#### ➕ Add New Client")
        with st.form("add_client_form"):
            ac1, ac2 = st.columns(2)
            with ac1:
                c_name = st.text_input("Client/Company Name")
                c_cat = st.selectbox("Category", CUSTOMER_CATEGORIES)
                c_city = st.text_input("City")
                c_state = st.selectbox("State", ["Gujarat", "Maharashtra", "Rajasthan", "MP", "Delhi", "Punjab", "Haryana", "UP", "South India", "Other"])
            with ac2:
                c_contact = st.text_input("Phone/Mobile")
                c_gst = st.text_input("GSTIN")
                c_addr = st.text_area("Full Address")
            
            if st.form_submit_button("Add Client"):
                if c_name:
                    add_customer(c_name, c_cat, c_city, c_state, c_contact, c_gst, c_addr)
                    st.success(f"Added {c_name}!")
                    st.rerun()
                else:
                    st.error("Name required.")
        
        # Excel Import (Compact)
        with st.expander("📤 Import from Excel (Bulk Upload)"):
            st.warning("Excel columns must include: Name, City, State, Contact")
            up_file = st.file_uploader("Upload Excel", type=['xlsx'])
            def_cat = st.selectbox("Default Category for Import", CUSTOMER_CATEGORIES)
            if up_file and st.button("Process Import"):
                cnt, msg = import_sales_from_excel(up_file, def_cat)
                if cnt > 0:
                    st.success(f"Imported {cnt} records!")
                    st.rerun()
                else:
                    st.error(f"Error: {msg}")

    # ======== 3. LOGISTICS & SERVICES ========
    with m_tab3:
        st.subheader("Manage Services (Transporters, CHA, Terminals)")
        
        services = load_services()
        
        # --- FILTERS & SORTING ---
        sf_1, sf_2, sf_3 = st.columns([2, 1, 1])
        with sf_1:
            serv_cat_filter = st.multiselect("Filter Service Type", SERVICE_CATEGORIES, default=["Transporter - Bulk"])
        with sf_2:
            serv_cities = sorted(list(set([s.get('city', '') for s in services if s.get('city')])))
            serv_city_filter = st.multiselect("Filter Hub/City", serv_cities)
        with sf_3:
             serv_sort = st.radio("Sort", ["A-Z", "Z-A"], horizontal=True, key="serv_sort")
            
        filtered_services = services
        # Apply Category Filter
        if not serv_cat_filter:
            serv_cat_filter = SERVICE_CATEGORIES
        filtered_services = [s for s in services if s.get('category') in serv_cat_filter]

        # Apply City Filter
        if serv_city_filter:
            filtered_services = [s for s in filtered_services if s.get('city') in serv_city_filter]
            
        # Apply Sorting
        rev_serv = True if serv_sort == "Z-A" else False
        filtered_services = sorted(filtered_services, key=lambda x: x.get('name', '').lower(), reverse=rev_serv)
        
        if filtered_services:
            srv_df = pd.DataFrame(filtered_services)
            # Standardize
            req_cols_s = ['name', 'category', 'city', 'contact', 'details']
            for col in req_cols_s:
                if col not in srv_df.columns:
                    srv_df[col] = ""
            
            display_s = srv_df[req_cols_s].copy()
            display_s.columns = ["Provider Name", "Service Type", "Hub / City", "Contact Info", "Details / Routes"]
            st.dataframe(display_s, use_container_width=True, hide_index=True)
        else:
            st.info("No service providers found matching criteria.")
            
        st.markdown("---")
        st.markdown("#### ➕ Add Service Provider")
        
        with st.form("add_service_form"):
            sc1, sc2 = st.columns(2)
            with sc1:
                svc_name = st.text_input("Name (Transporter/CHA/Person)")
                svc_cat = st.selectbox("Service Type", SERVICE_CATEGORIES)
                svc_city = st.text_input("Base Location/Port")
            with sc2:
                svc_contact = st.text_input("Contact Number")
                svc_details = st.text_area("Details (Vehicle Counts, Specialization, Loading Person Name)", help="Enter name of loading person or specific CHA contact here")
            
            if st.form_submit_button("Add Service Provider"):
                if svc_name:
                    add_service_provider(svc_name, svc_cat, svc_city, svc_contact, svc_details)
                    st.success("Added successfully!")
                    st.rerun()
                else:
                    st.error("Name required.")
        
        st.markdown("---")
        
        # Two columns: Bulk and Drum
        bulk_col, drum_col = st.columns(2)
        
        with bulk_col:
            st.markdown("### 🏭 Bulk Importers (10,000+ MT)")
            st.caption("Select parties for bulk purchase - Limited options")
            
            bulk_parties = load_suppliers()
            bulk_only = [p for p in bulk_parties if p['type'] in ['Bulk', 'Bulk/PSU']]
            
            for p in bulk_only:
                col_a, col_b, col_c = st.columns([0.5, 2, 1])
                with col_a:
                    checked = st.checkbox("Active", value=p.get('marked_for_purchase', True), key=f"bulk_{p['name'][:20]}", label_visibility="collapsed")
                    if checked != p.get('marked_for_purchase', True):
                        toggle_purchase_party(p['name'], checked)
                with col_b:
                    st.markdown(f"**{p['name']}**")
                    st.caption(f"📍 {p['city']} | 📦 {p['qty_mt']:,} MT")
                with col_c:
                    if checked:
                        st.success("✓ Active")
                    else:
                        st.warning("✗ Inactive")
        
        with drum_col:
            st.markdown("### 🛢️ Drum Importers (<10,000 MT)")
            st.caption("Open purchase from all - Scroll to view all")
            
            drum_only = [p for p in bulk_parties if p.get('type') in ['Drum', 'Indian Importer - Drum']]
            
            # Display in scrollable container
            if drum_only:
                drum_df = pd.DataFrame(drum_only)
                for col in ['name', 'qty_mt', 'city', 'marked_for_purchase']:
                     if col not in drum_df.columns:
                         drum_df[col] = False if col == 'marked_for_purchase' else ""
                
                drum_df = drum_df[['name', 'qty_mt', 'city', 'marked_for_purchase']]
                drum_df.columns = ['Company Name', 'Qty (MT)', 'City', 'Active']
                drum_df['Active'] = drum_df['Active'].apply(lambda x: '✓' if x else '✗')
                st.dataframe(drum_df, use_container_width=True, hide_index=True, height=400)
            else:
                st.info("No Drum Importers found.")
        
        # Add new purchase party
        st.markdown("---")
        st.subheader("➕ Add New Purchase Party")
        
        add_col1, add_col2, add_col3, add_col4 = st.columns(4)
        with add_col1:
            new_name = st.text_input("Company Name", key="new_purchase_name")
        with add_col2:
            new_type = st.selectbox("Type", SUPPLIER_CATEGORIES, key="new_purchase_type")
        with add_col3:
            new_city = st.selectbox("City", ["Mumbai", "Kandla", "Chennai", "Other"], key="new_purchase_city")
        with add_col4:
            new_qty = st.number_input("Est. Qty (MT)", min_value=0, value=1000, key="new_purchase_qty")
        
        add_col5, add_col6 = st.columns(2)
        with add_col5:
            new_contact = st.text_input("Contact Number", key="new_purchase_contact")
        with add_col6:
            new_gstin = st.text_input("GSTIN", key="new_purchase_gstin")
        
        if st.button("➕ Add Purchase Party", type="primary"):
            if new_name:
                # Using add_supplier wrapper
                add_supplier(new_name, new_type, new_city, new_contact, new_gstin)
                st.success(f"✅ Added {new_name} as {new_type} supplier!")
                st.rerun()
            else:
                st.error("Please enter company name")
    
    # ======== SALES PARTIES ========
    with m_tab2:
        st.subheader("💰 Sales Parties - Your Customers")
        
        sales_parties = load_customers()
        
        if sales_parties:
            sales_summary = {'total': len(sales_parties)}
            st.metric("Total Customers", sales_summary['total'])
            
            sales_df = pd.DataFrame(sales_parties)
            st.dataframe(sales_df, use_container_width=True, hide_index=True)
        else:
            st.info("No sales parties added yet. Add manually or import from Excel below.")
        
        # Add new sales party
        st.markdown("---")
        st.subheader("➕ Add New Sales Party / Customer")
        
        s_col1, s_col2, s_col3 = st.columns(3)
        with s_col1:
            s_name = st.text_input("Customer Name", key="new_sales_name")
            s_cat = st.selectbox("Category", CUSTOMER_CATEGORIES, key="new_sales_cat")
        with s_col2:
            s_city = st.text_input("City", key="new_sales_city")
            s_state = st.selectbox("State", ["Gujarat", "Maharashtra", "Rajasthan", "Madhya Pradesh", "Karnataka", "Tamil Nadu", "Andhra Pradesh", "Telangana", "Delhi", "Uttar Pradesh", "Bihar", "West Bengal", "Other"], key="new_sales_state")
            s_contact = st.text_input("Contact Number", key="new_sales_contact")
        with s_col3:
            s_gstin = st.text_input("GSTIN", key="new_sales_gstin")
            s_address = st.text_input("Address", key="new_sales_address")
        
        if st.button("➕ Add Customer", type="primary", key="add_sales_btn"):
            if s_name and s_city:
                # Using add_customer imported function
                add_customer(s_name, s_cat, s_city, s_state, s_contact, s_gstin, s_address)
                st.success(f"✅ Customer Saved (Added/Updated): {s_name} ({s_cat})")
                st.rerun()
            else:
                st.error("Please enter customer name and city")
    
    # ======== IMPORT/EXPORT ========
    with m_tab3:
        st.subheader("📤 Import / Export Party Data")
        
        imp_col, exp_col = st.columns(2)
        
        with imp_col:
            st.markdown("### 📥 Import Sales Parties from Excel")
            st.caption("Upload Excel file with columns: Company Name, City, State, Contact, GSTIN, Address")
            
            uploaded_file = st.file_uploader("Choose Excel file", type=['xlsx', 'xls'], key="sales_excel_upload")
            
            imp_def_cat = st.selectbox("Default Category (if missing in Excel)", CUSTOMER_CATEGORIES, key="imp_def_cat_sel")
            
            if uploaded_file is not None:
                if st.button("📥 Import & Merge Data"):
                    # Save temp file
                    import tempfile
                    with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as tmp:
                        tmp.write(uploaded_file.getvalue())
                        tmp_path = tmp.name
                    
                    msg, status = import_sales_from_excel(tmp_path, default_category=imp_def_cat)
                    
                    if status == "Success":
                        st.success(f"✅ Data Processed: {msg}")
                        st.rerun()
                    else:
                        st.error(f"❌ Import failed: {status}")
                    
                    os.remove(tmp_path)
            
            st.markdown("---")
            st.markdown("**Sample Excel Format:**")
            sample_data = {
                'Company Name': ['ABC Infra Ltd', 'XYZ Construction'],
                'City': ['Ahmedabad', 'Surat'],
                'State': ['Gujarat', 'Gujarat'],
                'Contact': ['9876543210', '9876543211'],
                'GSTIN': ['24ABCDE1234F1Z5', '24FGHIJ5678K2Z6'],
                'Address': ['123 Main Road', '456 Highway']
            }
            st.dataframe(pd.DataFrame(sample_data), use_container_width=True, hide_index=True)
        
        with exp_col:
            st.markdown("### 📤 Export Party Data")
            
            if st.button("📤 Export Purchase Parties"):
                purchase_df = pd.DataFrame(load_suppliers())
                st.download_button(
                    "⬇️ Download Purchase Parties CSV",
                    purchase_df.to_csv(index=False),
                    "purchase_parties.csv",
                    "text/csv"
                )
            
            if st.button("📤 Export Sales Parties"):
                sales_df = pd.DataFrame(load_sales_parties())
                if not sales_df.empty:
                    st.download_button(
                        "⬇️ Download Sales Parties CSV",
                        sales_df.to_csv(index=False),
                        "sales_parties.csv",
                        "text/csv"
                    )
                else:
                    st.warning("No sales parties to export")

# --- AI FALLBACK ENGINE (Multi-Provider: OpenAI→Ollama→HuggingFace→GPT4All→Claude) ---
if selected_page == "🔄 AI Fallback Engine":
    _render_page_header("🔄 AI Fallback Engine", "Knowledge & AI")
    try:
        from command_intel import ai_fallback_dashboard
        ai_fallback_dashboard.render()
    except Exception as _e:
        st.error(f"⚠️ AI Fallback Engine failed to load: {_e}")
        st.info("Try reloading the page. Ensure API keys are configured in ⚙️ Settings.")

# --- AI DASHBOARD ASSISTANT (Full Data-Connected AI) ---
elif selected_page == "🧠 AI Dashboard Assistant":
    _render_page_header("🧠 AI Dashboard Assistant", "Knowledge & AI")
    st.info(
        "ℹ️ **Department Head:** All Departments\n\n"
        "📌 AI assistant connected to ALL dashboard data — prices, APIs, competitors, "
        "contractors, demand, bugs, change log, forecasts. Role-based access control. "
        "Powered by Claude (Anthropic)."
    )
    try:
        from command_intel import ai_dashboard_assistant
        ai_dashboard_assistant.render()
    except Exception as _e:
        st.error(f"⚠️ AI Dashboard Assistant failed to load: {_e}")
        st.info("Ensure your Anthropic API key is set in ⚙️ Settings.")

# --- TAB 4: AI SALES ASSISTANT ---
elif selected_page == "🤖 AI Assistant":
    _render_page_header("🤖 AI Assistant", "Knowledge & AI")
    st.header("🤖 AI Sales Training & Assistant")
    st.caption("Instant answers, objection handling, and training manual.")

    try:
        from sales_knowledge_base import (
            get_chatbot_response, get_all_sections, get_section_questions,
            get_telecalling_script, polish_email, generate_custom_reply
        )
    except Exception as _e:
        st.error(f"⚠️ AI Assistant knowledge base failed to load: {_e}")
        st.stop()
    
    # Modes
    mode = st.radio("Select Mode", ["💡 Smart Q&A Search", "📚 Training Manual (Categorized)", "📞 Telecalling Scripts", "📧 AI Writing Helper"], horizontal=True)
    
    # 1. SMART Q&A
    if mode == "💡 Smart Q&A Search":
        st.markdown("ask me anything about bitumen sales, specs, or company policy...")
        query = st.text_input(" Enter question:", placeholder="e.g., Why is 100% advance payment required?")
        
        if query:
            resp = get_chatbot_response(query)
            if resp['found']:
                st.success(f"**ANSWER:** {resp['answer']}")
                st.caption(f"Section: {resp['section']} | Confidence: {resp['confidence']:.0f}%")
            else:
                st.warning("No specific training data found. Contact Senior Management.")
    
    # 2. TRAINING MANUAL
    elif mode == "📚 Training Manual (Categorized)":
        sections = get_all_sections()
        
        c1, c2 = st.columns([1, 2])
        
        with c1:
            st.markdown("#### Topics")
            selected_sec_key = st.radio("Select Topic", list(sections.keys()), format_func=lambda x: sections[x])
        
        with c2:
            st.markdown(f"#### 📖 {sections[selected_sec_key]}")
            questions = get_section_questions(selected_sec_key)
            
            for i, q in enumerate(questions):
                with st.expander(f"Q{i+1}: {q['question']}"):
                    st.markdown(f"**Answer:** {q['answer']}")
                    if 'keywords' in q:
                         st.caption(f"Keywords: {', '.join(q['keywords'])}")

    # 3. TELECALLING SCRIPTS
    elif mode == "📞 Telecalling Scripts":
        st.subheader("Telecalling Scripts")
        
        scenarios = {
            "intro": "1. Introduction (Cold Call)",
            "credibility": "2. Building Credibility",
            "qualification": "3. Qualification Questions",
            "objection_price": "4. Handling High Price Objection",
            "objection_credit": "5. Handling Credit/Payment Objection",
            "closing": "6. Closing / Asking for Order",
            "follow_up": "7. Follow Up Call"
        }
        
        selected_script = st.selectbox("Select Call Stage", list(scenarios.keys()), format_func=lambda x: scenarios[x])
        
        script_text = get_telecalling_script(selected_script)
        
        st.info("📢 **Read clearly and confidently:**")
        st.code(script_text, language="text")
        
        st.caption("Tip: Adapt the script to your natural style. Do not sound robotic.")

    # 4. AI WRITING HELPER
    elif mode == "📧 AI Writing Helper":
        st.subheader("✨ Professional Email & Message Assistant")
        
        ai_col1, ai_col2 = st.tabs(["✍️ Polish My Draft", "🚀 Auto-Generate Reply"])
        
        with ai_col1:
            st.caption("Paste your rough notes below, and AI will make it professional.")
            rough_txt = st.text_area("Your Rough Draft / Notes", height=150, placeholder="e.g. Hi sir rate is high due to crude oil. pls buy now otherwise next week price increase.")
            tone = st.radio("Select Tone", ["Professional", "Urgent", "Friendly"], horizontal=True)
            
            if st.button("✨ Polish Text"):
                if rough_txt:
                    polished = polish_email(rough_txt, tone)
                    st.markdown("### 📝 Polished Version:")
                    st.text_area("Copy this:", value=polished, height=200)
                else:
                    st.warning("Please enter some text first.")
                    
        with ai_col2:
            st.caption("Generate a reply for specific situations instantly.")
            gen_cust = st.text_input("Customer Name", value="Valued Customer")
            gen_topic = st.selectbox("Topic", ["Price Negotiation", "Supply Delay", "General Inquiry"])
            gen_context = st.text_input("Key Point (Reason)", placeholder="e.g. International Crude Rising")
            
            if st.button("🚀 Generate Reply"):
                reply = generate_custom_reply(gen_cust, gen_topic, gen_context)
                st.markdown("### 🤖 AI Suggestion:")
                st.text_area("Copy this:", value=reply, height=200)



    
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("Daily Broadcast")
        st.date_input("Select Date for Broadcast")
        st.text_area("Message Template", "Hello {name}, Great greetings! Today's best rate for {product} is {price}. Source: {source}.")
        st.button("🚀 Send Daily Updates (WhatsApp/SMS)")
        
    with c2:
        st.subheader("AI Voice Assistant")
        st.write("Automated calling for high-priority price changes.")
        st.toggle("Enable AI Calling for >5% Price Drop")
        st.button("Test AI Call to Myself")

# --- TAB 5: AUTOMATION RULES ---
# --- TAB 5: AUTOMATION RULES & SETTINGS ---
if selected_page == "⚙️ Settings":
    _render_page_header("⚙️ Settings", "Technology")
    st.header("⚙️ System Settings & Integrations")
    
    st.subheader("🔗 External Integrations & API Keys")
    st.caption("Manage connections to third-party services for automation.")
    
    with st.expander("🔑 API Configuration", expanded=True):
        api_c1, api_c2 = st.columns(2)
        with api_c1:
            try:
                from ai_fallback_engine import get_api_key as _get_ai_key
                _existing_openai_key = _get_ai_key("openai")
            except Exception:
                _existing_openai_key = ""
            openai_key_input = st.text_input("OpenAI / AI Key", value=_existing_openai_key, type="password", help="Required for AI Script generation (Optional).")
            st.text_input("Google Maps API Key", type="password", help="Required for accurate distance calculations.")
        with api_c2:
            st.caption("Email & WhatsApp configuration has moved to dedicated pages:")
            st.markdown("📧 **Email Engine** → SMTP Config tab")
            st.markdown("📱 **WhatsApp Engine** → API Config tab")

        if st.button("💾 Save API Settings"):
            if openai_key_input.strip():
                try:
                    from ai_fallback_engine import save_api_key as _save_ai_key
                    _save_ai_key("openai", openai_key_input.strip())
                    st.toast("OpenAI API key saved to ai_fallback_config.json")
                except Exception as _ke:
                    st.toast(f"Could not save key: {_ke}")
            else:
                st.toast("Settings Saved")

    with st.expander("🌐 Market Data API Keys", expanded=False):
        st.caption("Configure API keys for live market data feeds. All keys are stored locally in settings.json.")
        try:
            from settings_engine import load_settings as _ld_s, save_settings as _sv_s
            _api_sett = _ld_s()
            _api_configs = [
                ("EIA (US Energy)", "api_key_eia", "api_key_eia_enabled", "Crude oil & energy data from US EIA"),
                ("FRED (Federal Reserve)", "api_key_fred", "api_key_fred_enabled", "Economic indicators & interest rates"),
                ("data.gov.in", "api_key_data_gov_in", "api_key_data_gov_in_enabled", "Indian government open data"),
                ("OpenWeather", "api_key_openweather", "api_key_openweather_enabled", "Weather data for construction forecasting"),
                ("NewsAPI", "api_key_newsapi", "api_key_newsapi_enabled", "News headlines for sentiment analysis"),
            ]
            for _label, _key_field, _enable_field, _desc in _api_configs:
                _ac1, _ac2, _ac3 = st.columns([1.5, 2, 0.5])
                with _ac1:
                    _api_sett[_enable_field] = st.toggle(
                        f"Enable {_label}", value=_api_sett.get(_enable_field, False),
                        key=f"mkt_{_enable_field}")
                with _ac2:
                    _api_sett[_key_field] = st.text_input(
                        f"{_label} API Key", value=_api_sett.get(_key_field, ""),
                        type="password", key=f"mkt_{_key_field}", help=_desc)
                with _ac3:
                    if _api_sett.get(_key_field):
                        st.markdown("🟢")
                    else:
                        st.markdown("⚪")

            if st.button("💾 Save Market Data API Keys", key="save_mkt_api_keys"):
                _sv_s(_api_sett)
                # Also update hub_catalog.json if it exists
                try:
                    import json as _jj
                    _hc_path = Path("hub_catalog.json")
                    if _hc_path.exists():
                        with open(_hc_path, "r", encoding="utf-8") as _hf:
                            _hc = _jj.load(_hf)
                        _key_map = {
                            "api_key_eia": "eia", "api_key_fred": "fred",
                            "api_key_data_gov_in": "data_gov_in",
                            "api_key_openweather": "openweather",
                            "api_key_newsapi": "newsapi",
                        }
                        for _sf, _hid in _key_map.items():
                            for _entry in _hc:
                                if isinstance(_entry, dict) and _entry.get("id") == _hid:
                                    _entry["api_key"] = _api_sett.get(_sf, "")
                        with open(_hc_path, "w", encoding="utf-8") as _hf:
                            _jj.dump(_hc, _hf, indent=2, ensure_ascii=False)
                except Exception:
                    pass
                st.toast("Market Data API keys saved!")
        except Exception as _mke:
            st.caption(f"Settings engine unavailable: {_mke}")

    with st.expander("📧 Email & WhatsApp Automation", expanded=False):
        try:
            from settings_engine import load_settings as _load_s, save_settings as _save_s
            _sett = _load_s()
            _set_c1, _set_c2 = st.columns(2)
            with _set_c1:
                st.markdown("**Email Engine**")
                _sett["email_enabled"] = st.toggle("Enable Email Engine", value=_sett.get("email_enabled", False), key="set_email_en")
                _sett["email_auto_send_offer"] = st.toggle("Auto-send Offer Emails", value=_sett.get("email_auto_send_offer", False), key="set_email_offer")
                _sett["email_auto_send_followup"] = st.toggle("Auto-send Followup Emails", value=_sett.get("email_auto_send_followup", False), key="set_email_fu")
                _sett["email_auto_send_payment_reminder"] = st.toggle("Auto-send Payment Reminders", value=_sett.get("email_auto_send_payment_reminder", False), key="set_email_pay")
                _sett["email_director_report_enabled"] = st.toggle("Director Daily Report", value=_sett.get("email_director_report_enabled", False), key="set_email_dir")
                _sett["email_director_report_to"] = st.text_input("Director Report To", value=_sett.get("email_director_report_to", ""), key="set_email_dir_to")
            with _set_c2:
                st.markdown("**WhatsApp Engine**")
                _sett["whatsapp_enabled"] = st.toggle("Enable WhatsApp Engine", value=_sett.get("whatsapp_enabled", False), key="set_wa_en")
                _sett["whatsapp_auto_send_offer"] = st.toggle("Auto-send Offer Messages", value=_sett.get("whatsapp_auto_send_offer", False), key="set_wa_offer")
                _sett["whatsapp_auto_send_followup"] = st.toggle("Auto-send Followup Messages", value=_sett.get("whatsapp_auto_send_followup", False), key="set_wa_fu")
                _sett["whatsapp_auto_send_payment_reminder"] = st.toggle("Auto-send Payment Reminders (WA)", value=_sett.get("whatsapp_auto_send_payment_reminder", False), key="set_wa_pay")
                _sett["whatsapp_rate_limit_per_minute"] = st.number_input("Rate Limit (msgs/min)", value=_sett.get("whatsapp_rate_limit_per_minute", 20), key="set_wa_rate")
            if st.button("💾 Save Automation Settings", key="save_auto_settings"):
                _save_s(_sett)
                st.toast("Automation settings saved!")
        except Exception as _se:
            st.caption(f"Settings engine unavailable: {_se}")

    st.divider()
    
    st.subheader("🧠 Smart Logic Rules")
    st.write("Configure how the system reacts to market changes.")
    
    col_rule1, col_rule2 = st.columns(2)
    with col_rule1:
        st.checkbox("Auto-Switch if Stock Unavailable", value=True)
        st.checkbox("Alert if Transport Cost > 10% hike", value=True)
    with col_rule2:
        st.checkbox("Enable SOS Trigger (> ₹200 drop)", value=True)
        st.checkbox("Auto-Create CRM Tasks for New Leads", value=True)
    
    st.divider()
    st.subheader("🚫 Unavailability Overrides")
    st.multiselect("Mark these Ports as CURRENTLY OFFLINE:", ["Haldia", "Mumbai", "Chennai", "Kochi"])

# --- TAB 6: DATA MANAGER ---
if selected_page == "🛠️ Data Manager":
    _render_page_header("🛠️ Data Manager", "Operations")
    st.header("🛠️ Data Manager")
    st.info("Update live pricing, discounts, availability, and transport logistics.")
    
    if st.button("🔄 Reload Data & Clear Cache"):
        st.cache_resource.clear()
        st.rerun()
    
    dm1, dm2, dm3 = st.tabs(["🏭 Refinery & Price", "🚚 Logistics & Routes", "💰 Drum & Import Prices"])
    
    # --- SUB-TAB 1: REFINERY & PRICE ---
    with dm1:
        st.subheader("Update Source (Refinery) Data")
        all_sources = optimizer.get_all_sources()
        src = st.selectbox("Select Source / Refinery", all_sources)
        
        if src:
            mask = optimizer.df['source_location'] == src
            if mask.any():
                curr = optimizer.df[mask].iloc[0]
                
                st.markdown(f"**Current Status for: {src}**")
                
                c1, c2, c3 = st.columns(3)
                new_base_bulk = c1.number_input("Base Price (Bulk)", value=float(curr.get('base_bulk', 0)))
                new_disc_bulk = c2.number_input("Discount (Bulk)", value=float(curr.get('disc_bulk', 0)))
                is_active = c3.toggle("✅ Availability (On/Off)", value=bool(curr.get('is_active', 1)))
                
                c4, c5, c6 = st.columns(3)
                new_base_drum = c4.number_input("Base Price (Drum)", value=float(curr.get('base_drum', 0)))
                new_disc_drum = c5.number_input("Discount (Drum)", value=float(curr.get('disc_drum', 0)))
                
                if st.button("💾 Save Pricing Updates"):
                    success = optimizer.update_source_price(src, new_base_bulk, new_disc_bulk, is_active, new_base_drum, new_disc_drum)
                    if success:
                        st.success(f"✅ Updated Pricing & Availability for {src}!")
                        st.cache_resource.clear()
                    else:
                        st.error("Failed to update.")
    
    # --- SUB-TAB 2: LOGISTICS ---
    with dm2:
        st.subheader("Update Route Logistics")
        c_src, c_dest = st.columns(2)
        
        l_src = c_src.selectbox("Select Source", all_sources, key="log_src")
        
        all_cities = optimizer.get_cities()
        l_dest = c_dest.selectbox("Select Destination", all_cities, key="log_dest")
        
        if l_src and l_dest:
            mask = (optimizer.df['source_location'] == l_src) & (optimizer.df['destination'] == l_dest)
            
            if mask.any():
                curr_route = optimizer.df[mask].iloc[0]
                
                st.markdown("---")
                st.markdown(f"**Route: {l_src} ➝ {l_dest}**")
                
                col_d1, col_d2, col_d3 = st.columns(3)
                
                curr_dist = float(curr_route.get('distance_km', 0))
                new_dist = col_d1.number_input("📏 Distance (KM)", value=curr_dist)
                
                curr_rate = float(curr_route.get('rate_per_km', 0))
                if curr_rate == 0 and curr_dist > 0:
                     curr_rate = float(curr_route.get('transport_bulk', 0)) / curr_dist
                
                new_rate_bulk = col_d2.number_input("🚛 Bulk Rate (Rs/Ton/KM)", value=curr_rate, format="%.2f")
                
                curr_drum_cost = float(curr_route.get('transport_drum', 0))
                new_drum_cost = col_d3.number_input("🛢️ Drum One-Way Charge", value=curr_drum_cost)
                
                st.caption(f"Calculated Bulk Freight: {new_dist} km * {new_rate_bulk} = {format_inr(new_dist * new_rate_bulk)}")
                
                if st.button("💾 Save Logistics Updates"):
                     success = optimizer.update_route_logistics(l_src, l_dest, new_dist, new_rate_bulk, new_drum_cost)
                     if success:
                        st.success(f"✅ Updated Route: {l_src} -> {l_dest}")
                        st.cache_resource.clear()
            else:
                st.warning("Route not found in database.")
    
    # --- SUB-TAB 3: DRUM & IMPORT PRICES ---
    with dm3:
        st.subheader("💰 Drum Bitumen & Import Prices")
        st.info("Update drum prices for Mumbai and Kandla (only 2 loading points for drum)")
        
        # Load current prices
        live_prices = get_live_prices()
        
        st.markdown("### 🛢️ Drum Bitumen Prices (Per MT)")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**🚢 Mumbai**")
            mumbai_vg30 = st.number_input("Mumbai VG30 (Rs/MT)", value=float(live_prices.get('DRUM_MUMBAI_VG30', 37000)), key="mum_vg30")
            mumbai_vg10 = st.number_input("Mumbai VG10 (Rs/MT)", value=float(live_prices.get('DRUM_MUMBAI_VG10', 38000)), key="mum_vg10")
        
        with col2:
            st.markdown("**🚢 Kandla**")
            kandla_vg30 = st.number_input("Kandla VG30 (Rs/MT)", value=float(live_prices.get('DRUM_KANDLA_VG30', 35500)), key="kan_vg30")
            kandla_vg10 = st.number_input("Kandla VG10 (Rs/MT)", value=float(live_prices.get('DRUM_KANDLA_VG10', 36500)), key="kan_vg10")
        
        st.markdown("---")
        st.markdown("### 🔄 Decanter Settings")
        
        col3, col4 = st.columns(2)
        with col3:
            decanter_cost = st.number_input("Decanter Conversion Cost (Rs/MT)", value=float(live_prices.get('DECANTER_CONVERSION_COST', 500)), key="dec_cost")
        
        st.markdown("---")
        st.markdown("### 🚚 Transport Rates")
        
        col5, col6 = st.columns(2)
        with col5:
            bulk_rate = st.number_input("Bulk Rate (Rs/KM/MT)", value=float(live_prices.get('BULK_RATE_PER_KM', 5.5)), format="%.2f", key="bulk_rate")
        with col6:
            drum_rate = st.number_input("Drum Rate (Rs/KM/MT)", value=float(live_prices.get('DRUM_RATE_PER_KM', 6.0)), format="%.2f", key="drum_rate")
        
        if st.button("💾 Save All Prices", type="primary"):
            new_prices = {
                'DRUM_MUMBAI_VG30': mumbai_vg30,
                'DRUM_MUMBAI_VG10': mumbai_vg10,
                'DRUM_KANDLA_VG30': kandla_vg30,
                'DRUM_KANDLA_VG10': kandla_vg10,
                'DECANTER_CONVERSION_COST': decanter_cost,
                'BULK_RATE_PER_KM': bulk_rate,
                'DRUM_RATE_PER_KM': drum_rate
            }
            save_live_prices(new_prices)
            st.success("✅ All prices updated successfully!")
            st.cache_resource.clear()
            st.rerun()
        
        st.markdown("---")
        st.caption("📍 **Drum Loading Points**: Mumbai serves South India. Kandla serves North India, Gujarat & parts of Maharashtra.")

# --- CONTACT IMPORTER ---
if selected_page == "📥 Contact Importer":
    _render_page_header("📥 Contact Importer", "Operations")
    st.info("ℹ️ **Department Head:** Sales / CRM Admin\n\n📌 Upload PDF, Excel, CSV, or images — AI auto-extracts contacts and fills CRM fields.")
    try:
        from command_intel import contact_importer
        contact_importer.render()
    except Exception as _e:
        st.error(f"⚠️ Contact Importer failed to load: {_e}")
        st.info("Try reloading the page. If this persists, check 🏥 System Health.")

# --- TAB 7: SOURCE DIRECTORY ---
if selected_page == "📋 Source Directory":
    _render_page_header("📋 Source Directory", "Sales & Revenue")
    st.header("📋 Source Directory")
    st.info("Complete list of all loading points categorized by type.")
    
    dir_tab1, dir_tab2, dir_tab3 = st.tabs(["🏭 Indian Refineries (PSU)", "🚢 Import Terminals", "🔄 Private Decanters"])
    
    # --- Indian Refineries ---
    with dir_tab1:
        st.subheader("🏭 Indian Refineries (PSU)")
        st.caption("Oil Marketing Companies' refinery locations across India")
        
        ref_data = []
        for r in INDIAN_REFINERIES:
            ref_data.append({
                "Source Name": r['name'],
                "City": r['city'],
                "State": r['state'],
                "Type": "PSU Refinery"
            })
        
        st.dataframe(ref_data, use_container_width=True, hide_index=True)
        st.metric("Total Refineries", len(INDIAN_REFINERIES))
    
    # --- Import Terminals ---
    with dir_tab2:
        st.subheader("🚢 Import Terminals")
        st.caption("Ports and terminals for imported bulk bitumen")
        
        imp_data = []
        for r in IMPORT_TERMINALS:
            imp_data.append({
                "Terminal Name": r['name'],
                "City": r['city'],
                "State": r['state'],
                "Type": "Bulk Import"
            })
        
        st.dataframe(imp_data, use_container_width=True, hide_index=True)
        st.metric("Total Import Terminals", len(IMPORT_TERMINALS))
        
        st.markdown("---")
        st.markdown("**Key Import Ports:**")
        st.markdown("- 🚢 **Mangalore Port** - Major South India import hub")
        st.markdown("- 🚢 **Karwar Port** - Karnataka coast terminal")
        st.markdown("- 🚢 **Digi Port** - Gujarat import point")
        st.markdown("- 🚢 **Taloja Terminal** - Mumbai region storage")
        st.markdown("- 🚢 **VVF Mumbai** - Private import terminal")
    
    # --- Private Decanters ---
    with dir_tab3:
        st.subheader("🔄 Private Decanters")
        st.caption("Convert drum bitumen to bulk bitumen (decanting facilities)")
        
        dec_data = []
        for r in PRIVATE_DECANTERS:
            dec_data.append({
                "Decanter Name": r['name'],
                "City": r['city'],
                "State": r['state'],
                "Type": "Drum → Bulk"
            })
        
        st.dataframe(dec_data, use_container_width=True, hide_index=True)
        st.metric("Total Decanters", len(PRIVATE_DECANTERS))
        
        st.markdown("---")
        st.info("💡 **Decanter Info**: These facilities convert drum bitumen into bulk form for road construction projects.")

# --- TAB 8: FEASIBILITY ASSESSMENT ---
if selected_page == "🏭 Feasibility":
    _render_page_header("🏭 Feasibility", "Operations")
    st.header("📊 Feasibility Assessment")
    st.info("Automatic price comparison: **2 Refineries + 2 Import Terminals + 2 Decanters** for any destination")
    
    # Destination Selection
    all_destinations = sorted(list(DESTINATION_COORDS.keys()))
    selected_dest = st.selectbox("🎯 Select Destination City", all_destinations, key="feasibility_dest")
    
    if selected_dest:
        # Get assessment
        assessment = get_feasibility_assessment(selected_dest, top_n=2)
        
        if assessment:
            st.markdown(f"### 📍 Feasibility Report for: **{selected_dest}**")
            st.markdown("---")
            
            # 4 Column Layout for Categories
            col_ref, col_imp, col_dec, col_drum = st.columns(4)
            
            # --- REFINERIES ---
            with col_ref:
                st.markdown("#### 🏭 Refineries")
                for i, opt in enumerate(assessment['refineries']):
                    bg_color = "#D4EFDF" if i == 0 else "#EAFAF1"
                    border_color = "#196F3D" if i == 0 else "#A9DFBF"
                    
                    st.markdown(f'''
<div style="background-color:{bg_color}; border:2px solid {border_color}; color:#1e293b; padding:10px; border-radius:8px; margin-bottom:10px;">
<strong>#{i+1} {opt['source']}</strong><br>
<small>📏 {opt['distance_km']:.0f} KM</small><br>
<small>Base: {format_inr(opt['base_price'])}</small><br>
<span style="font-size:1.1em; font-weight:bold; color:#196F3D;">{format_inr(opt['landed_cost'])}</span>
</div>
''', unsafe_allow_html=True)
            
            # --- IMPORT TERMINALS ---
            with col_imp:
                st.markdown("#### 🚢 Import Bulk")
                for i, opt in enumerate(assessment['imports']):
                    bg_color = "#D6EAF8" if i == 0 else "#EBF5FB"
                    border_color = "#21618C" if i == 0 else "#AED6F1"
                    
                    st.markdown(f'''
<div style="background-color:{bg_color}; border:2px solid {border_color}; color:#1e293b; padding:10px; border-radius:8px; margin-bottom:10px;">
<strong>#{i+1} {opt['source']}</strong><br>
<small>📏 {opt['distance_km']:.0f} KM</small><br>
<small>Base: {format_inr(opt['base_price'])}</small><br>
<span style="font-size:1.1em; font-weight:bold; color:#21618C;">{format_inr(opt['landed_cost'])}</span>
</div>
''', unsafe_allow_html=True)
                    
            # --- LOCAL DECANTER ---
            with col_dec:
                st.markdown("#### 🔄 Decanter")
                dec = assessment.get('local_decanter')
                if dec:
                    st.markdown(f'''
<div style="background-color:#FCF3CF; border:2px solid #9A7D0A; padding:10px; border-radius:8px; margin-bottom:10px;">
<strong>{dec['source']}</strong><br>
<small>From: {dec.get('drum_source', 'N/A')}</small><br>
<small>Conv: {dec.get('conversion_cost', 500)}</small><br>
<span style="font-size:1.1em; font-weight:bold; color:#9A7D0A;">{format_inr(dec['landed_cost'])}</span>
</div>
''', unsafe_allow_html=True)
            
            # --- DRUM DIRECT ---
            with col_drum:
                st.markdown("#### 🛢️ Drum")
                drum = assessment.get('drum_direct')
                if drum:
                    st.markdown(f'''
<div style="background-color:#FADBD8; border:2px solid #C0392B; padding:10px; border-radius:8px; margin-bottom:10px;">
<strong>{drum['source']}</strong><br>
<small>📏 {drum['distance_km']:.0f} KM</small><br>
<small>Base: {format_inr(drum['base_price'])}</small><br>
<span style="font-size:1.1em; font-weight:bold; color:#C0392B;">{format_inr(drum['landed_cost'])}</span>
</div>
''', unsafe_allow_html=True)
            
            # Best Overall
            st.markdown("---")
            best = assessment['best_overall']
            if best:
                st.success(f"🏆 **BEST BULK OPTION**: {best['source']} @ **{format_inr(best['landed_cost'])}** PMT")
            
            # Full Comparison Table
            st.markdown("### 📋 Complete Comparison Table")
            comparison = get_comparison_table(selected_dest)
            if comparison:
                st.dataframe(comparison, use_container_width=True, hide_index=True)
            
            # Live Prices Info
            st.markdown("### 💰 Current Drum Prices")
            live = assessment.get('live_prices', {})
            col_p1, col_p2, col_p3 = st.columns(3)
            col_p1.metric("Mumbai Drum", f"{format_inr(live.get('drum_mumbai', 'N/A'))}")
            col_p2.metric("Kandla Drum", f"{format_inr(live.get('drum_kandla', 'N/A'))}")
            col_p3.metric("Decanter Cost", f"{live.get('decanter_cost', 'N/A')}")
        else:
            st.warning("Destination not found in database.")


# --- TAB 10: KNOWLEDGE BASE ---
if selected_page == "📚 Knowledge Base":
    _render_page_header("📚 Knowledge Base", "Knowledge & AI")
    st.header("📚 Bitumen Sales Knowledge Base")
    st.caption("Training Manual, FAQs, and Process Guidelines")

    try:
        from sales_knowledge_base import (
            TRAINING_SECTIONS, get_section_questions, get_knowledge_count
        )
    except Exception as _e:
        st.error(f"⚠️ Knowledge Base failed to load: {_e}")
        st.stop()
    
    # Header Stats
    kb_col1, kb_col2 = st.columns([3, 1])
    with kb_col1:
        st.markdown(f"**Total Q&A Topics:** {get_knowledge_count()}")
    with kb_col2:
        search_query = st.text_input("🔍 Search Knowledge Base", placeholder="e.g. payment terms...")
    
    st.markdown("---")
    
    # Search Logic
    if search_query:
        from sales_knowledge_base import get_chatbot_response
        response = get_chatbot_response(search_query)
        if response['found']:
            st.success(f"**Best Match for:** '{search_query}'")
            st.markdown(f"**Q:** {response['question']}")
            st.markdown(f"**A:** {response['answer']}")
            st.caption(f"Section: {response['section']} | Confidence: {response['confidence']:.0f}%")
        else:
            st.warning("No direct match found. Please browse the sections below.")
            
    # Display Categories
    for section_key, section_name in TRAINING_SECTIONS.items():
        questions = get_section_questions(section_key)
        count = len(questions)
        if count > 0:
            with st.expander(f"📌 {section_name} ({count} Qs)"):
                for item in questions:
                    st.markdown(f"**Q: {item['question']}**")
                    st.write(f"💡 {item['answer']}")
                    st.markdown("---")

    st.markdown("---")
    
    # --- TAB 11: CRM & TASKS (NEW) ---
if selected_page == "🎯 CRM & Tasks":
    _render_page_header("🎯 CRM & Tasks", "Sales & Revenue")
    st.header("🎯 Sales CRM & Daily Worklist")
    st.caption("Never miss a follow-up. Manage calls, tasks, and client engagement.")
    
    import crm_engine as crm
    
    # 1. KPI Header
    k1, k2, k3, k4 = st.columns(4)
    tasks_today = crm.get_due_tasks("Today")
    tasks_overdue = crm.get_due_tasks("Overdue")
    
    k1.metric("🔥 Hot Leads", "3", "Active")
    k2.metric("📅 Tasks Due Today", len(tasks_today), "For Action")
    k3.metric("⚠️ Overdue", len(tasks_overdue), "High Priority", delta_color="inverse")
    k4.metric("💰 Deals Closing", "2", "This Week")
    
    st.markdown("---")
    
    # 2. Main Worklist Interface
    crm_t1, crm_t2, crm_t3 = st.tabs(["📋 Today's Worklist", "📅 Calendar", "⚙️ Automation Rules"])
    
    with crm_t1:
        # Task Filter
        tf_col1, tf_col2 = st.columns([3, 1])
        with tf_col1:
            task_view = st.selectbox("View Tasks", ["Due Today", "Overdue", "Upcoming"])
        with tf_col2:
            if st.button("🔄 Refresh"): st.rerun()
            
        # Get Data
        current_tasks = crm.get_due_tasks(task_view.split(' ')[-1]) if "Due" not in task_view else crm.get_due_tasks("Today")
        if task_view == "Overdue": current_tasks = crm.get_due_tasks("Overdue")
        
        if not current_tasks:
            st.success("🎉 No tasks in this view! You are all caught up.")
        else:
            for t in current_tasks:
                with st.container():
                    # Card-like layout
                    c1, c2, c3, c4 = st.columns([2, 2, 2, 1])
                    with c1:
                        st.markdown(f"**{t['client']}**")
                        st.caption(f"📌 {t['note']}")
                    with c2:
                        st.write(f"**{t['type']}**")
                        st.caption(f"Due: {t['due_date']}")
                    with c3:
                        if t['priority'] == 'High': st.error("High Priority")
                        elif t['priority'] == 'Medium': st.warning("Medium")
                        else: st.info("Low")
                    with c4:
                        if st.button("✅ Done", key=f"done_{t['id']}"):
                            crm.complete_task(t['id'], "Marked done from Dashboard")
                            st.success("Task Closed!")
                            st.rerun()
                    st.markdown("---")
    
    with crm_t2:
        st.info("📅 Calendar View Coming Soon")
        
    with crm_t3:
        st.write("### 🤖 Automation Enabled")
        st.write("- **New Enquiry**: Auto-create Call Task (15min)")
        st.write("- **Quoted**: Auto-create Follow-up (2hr)")
        st.write("- **Payment**: Auto-create Daily Reminder")

    # 3. Quick Add Task
    with st.expander("➕ Add New Task Manually", expanded=False):
        with st.form("new_task_form"):
            nt_c1, nt_c2 = st.columns(2)
            nt_client = nt_c1.text_input("Client Name")
            nt_type = nt_c2.selectbox("Task Type", ["Call", "WhatsApp", "Email", "Meeting"])
            nt_due = st.date_input("Due Date")
            nt_note = st.text_area("Task Note")
            if st.form_submit_button("Add Task"):
                crm.add_task(nt_client, nt_type, str(nt_due) + " 09:00", "Medium", nt_note)
                st.success("Task Added!")
                st.rerun()

# --- TAB 12: SOS SPECIAL PRICE TRIGGER (NEW) ---
if selected_page == "🚨 SPECIAL PRICE (SOS)":
    _render_page_header("🚨 Special Price (SOS)", "Sales & Revenue", "URGENT")
    st.header("🚨 Special Price | SOS Sales Blast")
    st.caption("Active Special Price Opportunities (SPO) detected based on cost reduction.")
    
    import sos_engine as sos
    
    # 1. Active Opportunities
    opps = sos.get_active_sos()
    
    if not opps:
        st.info("No active SOS triggers at the moment. Market prices are stable.")
        if st.button("Simulate Price Drop (Demo)"):
            sos.create_sos_opportunity("Ahmedabad", "VG30", 42500, 41800)
            st.rerun()
    else:
        for opp in opps:
            with st.container():
                st.markdown(f"### 🔥 Location: {opp['location']} | Product: {opp['product']}")
                
                # Metrics
                m1, m2, m3, m4 = st.columns(4)
                m1.metric("📉 New Price", f"{format_inr(opp['new_price'])}")
                m2.metric("💰 Saving", f"{format_inr(opp['saving'])}", delta="Price Drop", delta_color="normal")
                m3.metric("⏳ Valid Until", opp['valid_until'].split(' ')[1]) # Time only
                m4.metric("👥 Targets", len(opp['target_customers']))
                
                st.markdown("#### 🎯 Target Customer List (Auto-Generated)")
                
                for cust in opp['target_customers']:
                    c1, c2, c3 = st.columns([2, 3, 2])
                    with c1:
                        st.write(f"**{cust['name']}**")
                        st.caption(f"📍 {cust.get('city', 'Unknown')} | Last: {cust['last_price']}")
                    with c2:
                        # Scripts
                        scripts = sos.generate_sos_script(cust['name'], opp['saving'], opp['new_price'], opp['valid_until'])
                        with st.popover("📞 View Call Script"):
                            st.write(scripts['call_script'])
                        with st.popover("💬 WhatsApp"):
                            st.code(scripts['whatsapp'])
                    with c3:
                        if st.button("✅ Called", key=f"sos_call_{opp['id']}_{cust['name']}"):
                            st.toast(f"Logged call to {cust['name']} ({cust.get('city')})")
                    st.divider()

# --- USER GUIDE SECTION ---
# --- TAB 9: REPORTS ---
if selected_page == "📤 Reports":
    _render_page_header("📤 Reports", "Finance")
    with st.container():
        st.header("📤 Sales Reports & Analytics")
        # st.info("Visual Analytics and Exportable Reports coming in Phase 3.")
        
        # Dashboard Overview
        r_col1, r_col2, r_col3 = st.columns(3)
        r_col1.metric("Total Sales (MT)", "4,250 MT", "+12%")
        r_col2.metric("Revenue (₹)", "₹ 18.2 Cr", "+8%")
        r_col3.metric("Avg Margin", "₹ 850/MT", "+5%")
        
        st.markdown("### 📊 Monthly Sales Trend")
        chart_data = pd.DataFrame(
            np.random.randn(12, 3),
            columns=['VG30', 'VG10', 'Emulsion']
        )
        # Mocking positive values
        chart_data = abs(chart_data) * 500 + 200
        chart_data.index = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
        
        st.bar_chart(chart_data)
        
        st.caption("Data source: Mock CRM Database")


        try:
            with open("DASHBOARD_USER_GUIDE.md", "r", encoding="utf-8") as f:
                user_guide_content = f.read()
            st.markdown(user_guide_content)
        except FileNotFoundError:
            st.error("User Manual file not found.")

# ========================================================================================
# COMMAND INTELLIGENCE SYSTEM — PANEL ROUTING
# ========================================================================================

if selected_page == "🌐 API Dashboard":
    _render_page_header("🌐 API Dashboard", "Technology")
    display_badge("real-time")
    try:
        api_dashboard.render()
    except Exception as _e:
        st.error(f"⚠️ API Dashboard failed to load: {_e}")
        st.info("Try reloading the page. If this persists, check 🏥 System Health.")
elif selected_page == "🔗 API HUB":
    _render_page_header("🔗 API HUB", "Technology", badge="Connections & Keys")
    try:
        api_hub_dashboard.render()
    except Exception as _e:
        st.error(f"⚠️ API HUB failed to load: {_e}")
        st.info("Try reloading the page.")
elif selected_page == "⚙️ Dev & System Activity":
    _render_page_header("⚙️ Dev & System Activity", "Technology")
    st.info("ℹ️ **Department Head:** CTO / Technology\n\n📌 Full audit trail of API changes, auto-repairs, errors, deployments, and system events. All timestamps in IST.")
    try:
        from command_intel import dev_activity
        dev_activity.render()
    except Exception as _e:
        st.error(f"⚠️ Dev & System Activity failed to load: {_e}")
        st.info("Try reloading the page.")
elif selected_page == "📁 PDF Archive":
    _render_page_header("📁 PDF Archive", "Technology")
    st.info("ℹ️ **Department Head:** Admin / CTO\n\n📌 All PDFs generated from dashboard pages — download, view metadata, or delete. Auto-saved whenever you use ⬇ PDF on any page.")
    try:
        from command_intel import pdf_archive
        pdf_archive.render()
    except Exception as _e:
        st.error(f"⚠️ PDF Archive failed to load: {_e}")
        st.info("Try reloading the page.")
elif selected_page == "🔮 Price Prediction":
    _render_page_header("🔮 Price Prediction", "Strategy & Intel")
    st.info("ℹ️ **UX Note:** Predicts global/local price trends focusing on the core 1st & 16th Indian cycles.")
    try:
        price_prediction.render()
    except Exception as _e:
        st.error(f"⚠️ Price Prediction failed to load: {_e}")
        st.info("Try reloading the page. If this persists, check 🏥 System Health.")
elif selected_page == "⏳ Past Predictions":
    _render_page_header("⏳ Past Predictions", "Strategy & Intel")
    st.info("ℹ️ **UX Note:** 10-Year historical truth table validating the quantitative MLR-DL model performance.")
    try:
        historical_revisions.render()
    except Exception as _e:
        st.error(f"⚠️ Past Predictions failed to load: {_e}")
        st.info("Try reloading the page.")
elif selected_page == "📝 Manual Price Entry":
    _render_page_header("📝 Manual Price Entry", "Strategy & Intel")
    st.info("ℹ️ **UX Note:** B2B override grid for field CRM quotes and real-time market entries.")
    try:
        manual_entry.render()
    except Exception as _e:
        st.error(f"⚠️ Manual Price Entry failed to load: {_e}")
        st.info("Try reloading the page.")
elif selected_page == "🔔 Change Notifications":
    _render_page_header("🔔 Change Notifications", "Technology")
    st.info("ℹ️ **UX Note:** Complete, immutable timeline log of every number change or API fallback occurrence.")
    try:
        change_log.render()
    except Exception as _e:
        st.error(f"⚠️ Change Notifications failed to load: {_e}")
        st.info("Try reloading the page.")
elif selected_page == "🏥 System Health":
    _render_page_header("🏥 System Health", "Technology", badge="SRE v1.0")
    try:
        sre_dashboard.render()
    except Exception as _e:
        st.error(f"⚠️ System Health failed to load: {_e}")
        st.info("Try reloading the page.")
elif selected_page == "🐞 Bug Tracker":
    _render_page_header("🐞 Bug Tracker", "Technology")
    st.info("ℹ️ **UX Note:** Health tracker for failed APIs, broken data models, and Dev-Ops diagnostics.")
    try:
        bug_tracker.render()
    except Exception as _e:
        st.error(f"⚠️ Bug Tracker failed to load: {_e}")
        st.info("Try reloading the page.")
elif selected_page == "📦 Import Cost Model":
    _render_page_header("📦 Import Cost Model", "Operations")
    st.info("ℹ️ **UX Note:** Shows granular breakdowns of import duties, logistics, and conversion costs from Port to Decanter.")
    try:
        import_cost_model.render()
    except Exception as _e:
        st.error(f"⚠️ Import Cost Model failed to load: {_e}")
        st.info("Try reloading the page.")
elif selected_page == "🛒 Purchase Advisor":
    _render_page_header("🛒 Purchase Advisor", "Procurement Intelligence")
    try:
        from command_intel.purchase_advisor_dashboard import render as _pa_render
        _pa_render()
    except Exception as _e:
        st.error(f"⚠️ Purchase Advisor failed to load: {_e}")
elif selected_page == "🚢 Maritime Logistics":
    _render_page_header("🚢 Maritime Logistics", "Operations")
    try:
        from command_intel import maritime_logistics_dashboard
        maritime_logistics_dashboard.render()
    except Exception as _e:
        st.error(f"⚠️ Maritime Logistics failed to load: {_e}")
elif selected_page == "🚢 Supply Chain":
    _render_page_header("🚢 Supply Chain", "Operations")
    st.info("ℹ️ **UX Note:** Tracks voyage ships, transit times, and potential local stock availability constraints.")
    try:
        supply_chain.render()
    except Exception as _e:
        st.error(f"⚠️ Supply Chain failed to load: {_e}")
        st.info("Try reloading the page.")
elif selected_page == "👷 Demand Analytics":
    _render_page_header("👷 Demand Analytics", "Finance")
    st.info("ℹ️ **UX Note:** Analyzes government and private contractor demand cycles based on infrastructure projects.")
    try:
        demand_analytics.render()
    except Exception as _e:
        st.error(f"⚠️ Demand Analytics failed to load: {_e}")
        st.info("Try reloading the page.")
elif selected_page == "💰 Financial Intelligence":
    _render_page_header("💰 Financial Intelligence", "Finance")
    st.info("ℹ️ **UX Note:** Provides insights on buyer credit, rolling capital management, and payment cycles.")
    try:
        financial_intel.render()
    except Exception as _e:
        st.error(f"⚠️ Financial Intelligence failed to load: {_e}")
        st.info("Try reloading the page.")
elif selected_page == "🛡️ GST & Legal Monitor":
    _render_page_header("🛡️ GST & Legal Monitor", "Legal & Compliance")
    st.info("ℹ️ **UX Note:** Updates on regulatory changes, e-way bills, and compliance risk related to Bitumen sales.")
    try:
        gst_legal_monitor.render()
    except Exception as _e:
        st.error(f"⚠️ GST & Legal Monitor failed to load: {_e}")
        st.info("Try reloading the page.")
elif selected_page == "⚡ Risk Scoring":
    _render_page_header("⚡ Risk Scoring", "Legal & Compliance")
    st.info("ℹ️ **UX Note:** Gives an AI-driven composite Risk Score (1-100) combining counterparty defaults and market volatility.")
    try:
        risk_scoring.render()
    except Exception as _e:
        st.error(f"⚠️ Risk Scoring failed to load: {_e}")
        st.info("Try reloading the page.")
elif selected_page == "🔔 Alert System":
    _render_page_header("🔔 Alert System", "Strategy & Intel")
    st.info("ℹ️ **UX Note:** Configurable notifications for price drops, supply shocks, and new opportunities.")
    try:
        alert_system.render()
    except Exception as _e:
        st.error(f"⚠️ Alert System failed to load: {_e}")
        st.info("Try reloading the page.")
elif selected_page == "🎯 Strategy Panel":
    _render_page_header("🎯 Strategy Panel", "Strategy & Intel")
    st.info("ℹ️ **UX Note:** High-level strategic synthesis and \"What-If\" scenarios for volume purchasing.")
    try:
        strategy_panel.render()
    except Exception as _e:
        st.error(f"⚠️ Strategy Panel failed to load: {_e}")
        st.info("Try reloading the page.")
elif selected_page == "🏛️ Business Intelligence":
    _render_page_header("🏛️ Business Intelligence", "Knowledge & AI")
    st.info("ℹ️ **Department Head:** All Departments\n\n📌 Complete guide for every dashboard tab — for employees, partners, investors, and auditors.")
    try:
        from business_knowledge_base import render as biz_kb_render
        biz_kb_render()
    except Exception as _e:
        st.error(f"⚠️ Business Intelligence failed to load: {_e}")
        st.info("Try reloading the page.")
elif selected_page == "📰 News Intelligence":
    _render_page_header("📰 News Intelligence", "Market Intel")
    st.info("ℹ️ **Department Head:** Strategy Director / Business Intelligence\n\n📌 Live news aggregator — 14 sources (RSS + API), auto-classified by keywords, impact-scored 0–100. Breaking alerts for score ≥ 80.")
    try:
        from command_intel import news_dashboard as _nd
        _nd.render()
    except Exception as _e:
        st.error(f"⚠️ News Intelligence failed to load: {_e}")
        st.info("Try reloading the page.")
elif selected_page == "🕵️ Competitor Intelligence":
    _render_page_header("🕵️ Competitor Intelligence", "Market Intel")
    st.info("ℹ️ **Department Head:** Strategy Director / Sales Head\n\n📌 Daily forecasts from Multi Energy Enterprises (MEE, Mumbai) — cross-verified against IOCL/HPCL circulars, international bitumen FOB prices, India consumption/production stats, and industry news.")
    try:
        import competitor_intelligence
        competitor_intelligence.render()
    except Exception as _e:
        st.error(f"⚠️ Competitor Intelligence failed to load: {_e}")
        st.info("Try reloading the page.")
elif selected_page == "🔭 Contractor OSINT":
    _render_page_header("🔭 Contractor OSINT", "Market Intel")
    st.info("ℹ️ **Department Head:** Sales Director / Business Development\n\n📌 Track road contractors — project awards, bitumen demand (MT), monthly heatmap, risk flags, CRM export. Last 6 months only.")
    try:
        import contractor_osint
        contractor_osint.render()
    except Exception as _e:
        st.error(f"⚠️ Contractor OSINT failed to load: {_e}")
        st.info("Try reloading the page.")
elif selected_page == "🛣️ Road Budget & Demand":
    _render_page_header("🛣️ Road Budget & Demand", "Market Intel")
    st.info("ℹ️ **Department Head:** Strategy Director\n\n📌 India road budget analysis, state-wise bitumen demand forecast, and construction seasonality.")
    try:
        from command_intel import road_budget_demand
        road_budget_demand.render()
    except Exception as _e:
        st.error(f"⚠️ Road Budget & Demand failed to load: {_e}")
        st.info("Try reloading the page.")
elif selected_page == "🏗️ Govt Data Hub":
    _render_page_header("🏗️ Govt Data Hub", "Market Intel", badge="Phase 1 Live")
    st.info("ℹ️ **Department Head:** Strategy Director\n\n📌 Government & inter-governmental data feeds — NHAI highway progress, UN Comtrade HS 271320 imports, FX rates, PPAC reference. Excel Power Query templates included.")
    try:
        govt_hub_dashboard.render()
    except Exception as _e:
        st.error(f"⚠️ Govt Data Hub failed to load: {_e}")
        st.info("Try reloading the page.")
elif selected_page == "⚓ Port Import Tracker":
    _render_page_header("⚓ Port Import Tracker", "Market Intel")
    st.info("ℹ️ **Department Head:** Trade Analytics\n\n📌 Country-wise HS 271320 imports allocated to Indian ports with confidence scoring. Editable allocation rules via PORT MAPPING HUB.")
    try:
        port_tracker_dashboard.render()
    except Exception as _e:
        st.error(f"⚠️ Port Import Tracker failed to load: {_e}")
        st.info("Try reloading the page.")
elif selected_page == "📈 Demand Correlation":
    _render_page_header("📈 Demand Correlation", "Market Intel", badge="β Model")
    st.info("ℹ️ **Department Head:** Quantitative Strategy\n\n📌 Highway KM vs Bitumen Demand cross-correlation (Pearson, lag 0-12m) + OLS regression model. Auto-generated insights and divergence alerts.")
    try:
        correlation_dashboard.render()
    except Exception as _e:
        st.error(f"⚠️ Demand Correlation failed to load: {_e}")
        st.info("Try reloading the page.")
elif selected_page == "🗂️ India Procurement Directory":
    _render_page_header("🗂️ India Procurement Directory", "Market Intel", badge="Phase 1")
    st.info(
        "ℹ️ Official contacts for all Central & State govt road/bitumen procurement bodies. "
        "Phase 1: Central Ministries, NHAI Regional Offices, all State/UT PWD HQs, "
        "and State Road Corporations. Sources: official govt websites only."
    )
    try:
        directory_dashboard.render()
    except Exception as _e:
        st.error(f"⚠️ India Procurement Directory failed to load: {_e}")
        st.info("Try reloading the page.")

# ========================================================================================
# NEW INTELLIGENCE PAGES — Opportunities, Negotiation, Communication, Sync
# ========================================================================================

elif selected_page == "🔍 Opportunities":
    _render_page_header("🔍 Opportunities (Auto-Discovered)", "Intelligence", badge="AI-Powered")
    st.info("Auto-discovers profitable opportunities from market changes. Scan runs daily + on price changes.")
    try:
        from opportunity_engine import OpportunityEngine, get_all_opportunities, mark_opportunity_status
        import json as _opp_json

        _opp_eng = OpportunityEngine()
        _opp_tabs = st.tabs(["New Opportunities", "Today's Targets", "Scan Now", "All History"])

        with _opp_tabs[0]:
            _new_opps = get_all_opportunities(status="new")
            if _new_opps:
                st.markdown(f"**{len(_new_opps)} new opportunities found**")
                for _oi, _opp in enumerate(_new_opps[:20]):
                    _otype = _opp.get("type", "")
                    _otitle = _opp.get("title", "Opportunity")
                    _opri = _opp.get("priority", "P2")
                    _pri_colors = {"P0": "🔴", "P1": "🟡", "P2": "🔵"}
                    with st.expander(f"{_pri_colors.get(_opri, '🔵')} [{_opri}] {_otitle}"):
                        st.markdown(_opp.get("description", ""))
                        if _opp.get("recommended_action"):
                            st.success(f"**Action:** {_opp['recommended_action']}")
                        _oc1, _oc2, _oc3 = st.columns(3)
                        _oc1.metric("Savings/MT", _fmt_inr_home(_opp.get("savings_per_mt", 0)))
                        _oc2.metric("Est. Margin", _fmt_inr_home(_opp.get("estimated_margin_per_mt", 0)))
                        _oc3.metric("Est. Volume", f"{_opp.get('estimated_volume_mt', 0):.0f} MT")
                        if _opp.get("whatsapp_template"):
                            with st.expander("WhatsApp Template"):
                                st.code(_opp["whatsapp_template"], language=None)
                        if _opp.get("call_script"):
                            with st.expander("Call Script"):
                                st.code(_opp["call_script"], language=None)
                        if st.button(f"Mark Contacted", key=f"opp_contact_{_oi}"):
                            # Find real index in full opportunity list
                            _all_opps_list = get_all_opportunities()
                            for _real_idx, _real_opp in enumerate(_all_opps_list):
                                if (_real_opp.get("title") == _opp.get("title")
                                        and _real_opp.get("created_at") == _opp.get("created_at")):
                                    mark_opportunity_status(_real_idx, "contacted")
                                    break
                            st.rerun()
            else:
                st.info("No new opportunities. Click 'Scan Now' to run a fresh scan.")

        with _opp_tabs[1]:
            _recs = _opp_eng.get_todays_recommendations()
            st.subheader("Buyers to Call Today")
            _b2c = _recs.get("buyers_to_call", [])
            if _b2c:
                for _bc in _b2c[:10]:
                    st.markdown(f"- **{_bc.get('customer_name', 'Unknown')}** ({_bc.get('customer_city', '')}) — "
                                f"Save {_fmt_inr_home(_bc.get('savings_per_mt', 0))}/MT | {_bc.get('priority', 'P2')}")
            else:
                st.caption("No buyers flagged. Run a scan first.")

            st.subheader("Follow-ups Due")
            _fud = _recs.get("followups_due", [])
            if _fud:
                for _fu in _fud[:10]:
                    st.markdown(f"- {_fu.get('title', _fu.get('customer_name', 'Task'))} — {_fu.get('status', '')}")
            else:
                st.caption("No follow-ups due.")

            st.subheader("Reactivation Targets")
            _react = _recs.get("reactivation_targets", [])
            if _react:
                for _rt in _react[:5]:
                    st.markdown(f"- **{_rt.get('customer_name', _rt.get('title', 'Target'))}** — {_rt.get('type', '')}")
            else:
                st.caption("No reactivation targets found.")

        with _opp_tabs[2]:
            st.markdown("Run a fresh opportunity scan across all data sources.")
            if st.button("Run Full Opportunity Scan", type="primary", use_container_width=True):
                with st.spinner("Scanning for opportunities..."):
                    _scan_results = _opp_eng.scan_all_opportunities()
                st.success(f"Scan complete! Found {len(_scan_results)} opportunities.")
                st.rerun()

        with _opp_tabs[3]:
            _all_opps = get_all_opportunities()
            if _all_opps:
                _opp_df = pd.DataFrame(_all_opps)
                _cols_to_show = ["type", "title", "priority", "status", "savings_per_mt", "created_at"]
                _display_cols = [c for c in _cols_to_show if c in _opp_df.columns]
                st.dataframe(_opp_df[_display_cols], use_container_width=True, hide_index=True)
            else:
                st.info("No opportunity history yet.")

    except Exception as _e:
        st.error(f"Opportunities failed to load: {_e}")

elif selected_page == "🤝 Negotiation Assistant":
    _render_page_header("🤝 Negotiation Assistant", "Sales & CRM", badge="AI-Powered")
    st.info("Prepares complete briefing packs for sales team before customer calls.")
    try:
        from negotiation_engine import NegotiationAssistant, get_full_objection_library

        _neg = NegotiationAssistant()
        _neg_tabs = st.tabs(["Prepare Brief", "Objection Library"])

        with _neg_tabs[0]:
            st.subheader("Customer Negotiation Brief")
            _nc1, _nc2 = st.columns(2)
            with _nc1:
                _neg_cust = st.text_input("Customer Name", placeholder="e.g. L&T Construction")
                _neg_city = st.text_input("City", placeholder="e.g. Ahmedabad")
            with _nc2:
                _neg_grade = st.selectbox("Grade", ["VG30", "VG10", "VG40", "CRMB-55", "CRMB-60", "PMB", "Emulsion"])
                _neg_qty = st.number_input("Quantity (MT)", min_value=10, max_value=10000, value=100, step=10)
            _neg_last_price = st.number_input("Customer's Last Purchase Price (INR/MT, optional)", min_value=0, value=0, step=500)

            if st.button("Generate Negotiation Brief", type="primary", use_container_width=True):
                if _neg_cust and _neg_city:
                    with st.spinner("Preparing negotiation brief..."):
                        _brief = _neg.prepare_negotiation_brief(
                            customer_name=_neg_cust,
                            city=_neg_city,
                            grade=_neg_grade,
                            quantity_mt=_neg_qty,
                            customer_last_price=_neg_last_price if _neg_last_price > 0 else None
                        )

                    # Display brief
                    st.subheader("Negotiation Brief")

                    # Customer Profile
                    _cp = _brief.get("customer_profile", {})
                    st.markdown(f"**Customer:** {_cp.get('name', _neg_cust)} | "
                                f"**City:** {_cp.get('city', _neg_city)} | "
                                f"**Stage:** {_cp.get('relationship', 'new')}")

                    # Cost & Offers
                    _cost = _brief.get("our_best_cost", {})
                    if _cost:
                        _bc1, _bc2 = st.columns(2)
                        _bc1.metric("Best Landed Cost", f"{_fmt_inr_home(_cost.get('landed_cost', 0))}/MT")
                        _bc1.caption(f"Source: {_cost.get('source', 'N/A')}")
                        if _brief.get("walk_away_price"):
                            _bc2.metric("Walk-Away Price", _brief["walk_away_price"]["label"])

                    # Offer Tiers
                    _tiers = _brief.get("offer_tiers")
                    if _tiers:
                        st.markdown("**3-Tier Offer Pricing:**")
                        _tc1, _tc2, _tc3 = st.columns(3)
                        for _col, _tier_key, _color in [(_tc1, "aggressive", "#2d6a4f"), (_tc2, "balanced", "#1e3a5f"), (_tc3, "premium", "#c9a84c")]:
                            _tier = _tiers.get(_tier_key, {})
                            _col.markdown(f"""
<div style="background:#faf7f2;border:2px solid {_color};border-radius:8px;padding:12px;text-align:center;">
  <div style="font-size:0.68rem;font-weight:700;color:{_color};text-transform:uppercase;">{_tier.get('label', _tier_key)}</div>
  <div style="font-size:1.2rem;font-weight:800;color:#1e3a5f;">{_fmt_inr_home(_tier.get('price', 0))}</div>
  <div style="font-size:0.72rem;color:#64748b;">Margin: {_fmt_inr_home(_tier.get('margin', 0))}/MT</div>
</div>""", unsafe_allow_html=True)

                    # Client Benefit
                    _cb = _brief.get("client_benefit")
                    if _cb:
                        st.info(_cb.get("narrative", ""))

                    # Market Context
                    _mc = _brief.get("market_context", {})
                    if _mc.get("narrative"):
                        st.markdown(f"**Market Context:** {_mc['narrative']}")

                    # Objection Handling
                    _objs = _brief.get("objection_handling", [])
                    if _objs:
                        st.markdown("**Top Objections & Responses:**")
                        for _obj in _objs:
                            with st.expander(f"{_obj['objection']}"):
                                st.markdown(f"**Quick Reply:** {_obj['short_reply']}")
                                st.markdown(f"**Detailed:** {_obj['detailed_reply']}")
                                st.caption(f"Confidence: {_obj['confidence_booster']}")

                    # Closing Strategy
                    _cs = _brief.get("closing_strategy", {})
                    if _cs.get("recommended"):
                        st.markdown(f"**Recommended Close:** {_cs['recommended']['technique']}")
                        st.success(f'"{_cs["recommended"]["script"]}"')
                else:
                    st.warning("Please enter customer name and city.")

        with _neg_tabs[1]:
            st.subheader("Complete Objection Library")
            _obj_lib = get_full_objection_library()
            for _ok, _ov in _obj_lib.items():
                with st.expander(f"{_ov['objection']}"):
                    st.markdown(f"**Quick Reply:** {_ov['short_reply']}")
                    st.markdown(f"**Detailed:** {_ov['detailed_reply']}")
                    st.caption(f"Confidence Booster: {_ov['confidence_booster']}")

    except Exception as _e:
        st.error(f"Negotiation Assistant failed to load: {_e}")

elif selected_page == "💬 Communication Hub":
    _render_page_header("💬 Communication Hub", "Sales & CRM", badge="Templates")
    st.info("Auto-generates WhatsApp, Email, and Call scripts for sales team.")
    try:
        from communication_engine import CommunicationHub

        _comm = CommunicationHub()
        _comm_tabs = st.tabs(["Generate Message", "Follow-up Sequence", "Communication Log"])

        with _comm_tabs[0]:
            _msg_type = st.selectbox("Message Type", ["Offer", "Follow-up", "Reactivation", "Payment Reminder"])
            _cc1, _cc2 = st.columns(2)
            with _cc1:
                _comm_cust = st.text_input("Customer Name", placeholder="e.g. L&T Construction", key="comm_cust")
                _comm_city = st.text_input("City", placeholder="e.g. Mumbai", key="comm_city")
            with _cc2:
                _comm_grade = st.selectbox("Grade", ["VG30", "VG10", "VG40"], key="comm_grade")
                _comm_qty = st.number_input("Quantity (MT)", min_value=10, value=100, step=10, key="comm_qty")
            _comm_price = st.number_input("Price (INR/MT)", min_value=20000, max_value=80000, value=42000, step=500, key="comm_price")

            _channel = st.radio("Channel", ["WhatsApp", "Email", "Call Script"], horizontal=True)

            if st.button("Generate", type="primary", use_container_width=True, key="comm_gen"):
                if _comm_cust:
                    if _channel == "WhatsApp":
                        if _msg_type == "Offer":
                            _msg = _comm.whatsapp_offer(_comm_cust, _comm_city, _comm_grade, _comm_qty, _comm_price)
                        elif _msg_type == "Follow-up":
                            _msg = _comm.whatsapp_followup(_comm_cust)
                        elif _msg_type == "Reactivation":
                            _msg = _comm.whatsapp_reactivation(_comm_cust, _comm_city, _comm_price, _comm_price + 2000, 2000)
                        else:
                            _msg = _comm.whatsapp_payment_reminder(_comm_cust, 500000)
                        st.text_area("WhatsApp Message (copy & send):", _msg, height=300)
                    elif _channel == "Email":
                        if _msg_type == "Offer":
                            _email = _comm.email_offer(_comm_cust, _comm_city, _comm_grade, _comm_qty, _comm_price)
                        else:
                            _email = _comm.email_followup(_comm_cust, city=_comm_city, price=_comm_price)
                        st.text_input("Subject:", _email.get("subject", ""), key="email_subj_out")
                        st.text_area("Body:", _email.get("body", ""), height=350)
                    else:
                        _script = _comm.call_script_offer(_comm_cust, _comm_city, _comm_grade, _comm_price)
                        st.text_area("Call Script:", _script, height=400)

                    _comm.log_communication(_comm_cust, _channel, _msg_type)
                    st.success(f"{_channel} {_msg_type} generated and logged.")
                else:
                    st.warning("Please enter customer name.")

        with _comm_tabs[1]:
            st.subheader("5-Touch Follow-up Sequence")
            _fu_cust = st.text_input("Customer", key="fu_cust")
            _fu_city = st.text_input("City", key="fu_city")
            _fu_price = st.number_input("Offer Price", min_value=20000, value=42000, step=500, key="fu_price")
            if st.button("Generate Sequence", key="fu_gen"):
                if _fu_cust:
                    _seq = _comm.generate_followup_sequence(_fu_cust, _fu_city, _fu_price)
                    for _s in _seq:
                        _day_label = f"Day {_s['day']}" if _s['day'] > 0 else "Today"
                        st.markdown(f"**{_day_label}** — {_s['channel']}: {_s['action']}")

        with _comm_tabs[2]:
            st.subheader("Recent Communications")
            _hist = _comm.get_communication_history(limit=30)
            if _hist:
                _hist_df = pd.DataFrame(_hist)
                st.dataframe(_hist_df, use_container_width=True, hide_index=True)
            else:
                st.caption("No communications logged yet.")

    except Exception as _e:
        st.error(f"Communication Hub failed to load: {_e}")

elif selected_page == "🔄 Sync Status":
    _render_page_header("🔄 Sync Status", "System", badge="Auto-Sync")
    st.info("Master data synchronization — runs automatically every 60 minutes + on-demand.")
    try:
        from sync_engine import SyncEngine
        import json as _sync_json

        _sync_eng = SyncEngine()
        _sync_tabs = st.tabs(["Run Sync", "Sync History", "Missing Inputs"])

        with _sync_tabs[0]:
            st.subheader("Manual Sync")
            _sc1, _sc2 = st.columns(2)
            with _sc1:
                if st.button("Full Sync (All Sources)", type="primary", use_container_width=True):
                    with st.spinner("Running full sync... this may take 2-3 minutes"):
                        _result = _sync_eng.run_full_sync()
                    st.success(f"Sync completed! {_result.get('apis_succeeded', 0)} APIs succeeded.")
                    for _step in _result.get("steps", []):
                        _step_status = _step.get("status", "unknown")
                        _step_icon = "OK" if _step_status == "success" else "WARN" if _step_status == "partial" else "FAIL"
                        st.markdown(f"- [{_step_icon}] {_step.get('name', 'Step')} — {_step.get('details', '')}")
            with _sc2:
                if st.button("Market Data Only (Quick)", use_container_width=True):
                    with st.spinner("Syncing market data..."):
                        _result = _sync_eng.run_market_only()
                    st.success("Market sync completed!")

        with _sync_tabs[1]:
            st.subheader("Sync History")
            try:
                from database import get_sync_logs
                _logs = get_sync_logs(limit=20)
                if _logs:
                    _log_df = pd.DataFrame(_logs)
                    st.dataframe(_log_df, use_container_width=True, hide_index=True)
                else:
                    st.caption("No sync history yet. Run a sync first.")
            except Exception:
                st.caption("Sync logs not available.")

        with _sync_tabs[2]:
            st.subheader("Missing Data Inputs")
            try:
                from missing_inputs_engine import MissingInputsEngine
                _mi_eng = MissingInputsEngine()
                _gaps = _mi_eng.scan_all_gaps()
                if _gaps:
                    st.markdown(f"**{len(_gaps)} data gaps detected:**")
                    for _g in _gaps:
                        _gpri = _g.get("priority", "Medium")
                        _gpri_clr = "#dc2626" if _gpri == "High" else "#d97706" if _gpri == "Medium" else "#3b82f6"
                        st.markdown(f"""
<div style="display:flex;justify-content:space-between;align-items:center;
            padding:6px 10px;border-bottom:1px solid #f1f5f9;font-size:0.82rem;">
  <div>
    <span style="color:#2d3142;font-weight:600;">{_g.get('label', _g.get('field', 'Unknown'))}</span>
    <span style="font-size:0.72rem;color:#64748b;margin-left:8px;">{_g.get('reason', '')}</span>
  </div>
  <span style="background:{_gpri_clr};color:#fff;font-size:0.65rem;font-weight:700;
               padding:2px 8px;border-radius:8px;">{_gpri}</span>
</div>""", unsafe_allow_html=True)

                    _daily_qs = _mi_eng._daily_questions()
                    if _daily_qs:
                        st.subheader("Daily Questions")
                        for _dq in _daily_qs:
                            st.text_input(_dq.get("label", ""), key=f"dq_{_dq.get('field', '')}", placeholder=_dq.get("placeholder", ""))
                else:
                    st.success("No data gaps detected! All inputs are up to date.")
            except Exception as _e:
                st.caption(f"Missing inputs scanner not available: {_e}")

    except Exception as _e:
        st.error(f"Sync Status failed to load: {_e}")


# ── NEW PAGES: Director Briefing, Email Engine, WhatsApp Engine, Daily Log,
#    Alert Center, AI Learning ─────────────────────────────────────────────────

elif selected_page == "📋 Director Briefing":
    _render_page_header("📋 Director Briefing", "Executive", badge="Daily Intel")
    try:
        cmd_director_dashboard.render()
    except Exception as _e:
        st.error(f"Director Briefing failed to load: {_e}")

elif selected_page in ("📧 Email Setup", "📧 Email Engine"):
    _render_page_header("📧 Email Setup", "Sales & CRM", badge="SMTP Queue")
    try:
        from command_intel import email_setup_dashboard
        email_setup_dashboard.render()
    except Exception as _e:
        st.error(f"Email Setup failed to load: {_e}")

elif selected_page in ("📱 WhatsApp Setup", "📱 WhatsApp Engine"):
    _render_page_header("📱 WhatsApp Setup", "Sales & CRM", badge="360dialog")
    try:
        from command_intel import whatsapp_setup_dashboard
        whatsapp_setup_dashboard.render()
    except Exception as _e:
        st.error(f"WhatsApp Setup failed to load: {_e}")

elif selected_page == "🔗 Integrations":
    _render_page_header("🔗 Integrations", "System", badge="Connections")
    try:
        from command_intel import integrations_dashboard
        integrations_dashboard.render()
    except Exception as _e:
        st.error(f"Integrations failed to load: {_e}")

elif selected_page == "📊 Comm Tracking":
    _render_page_header("📊 Communication Tracking", "Sales & CRM", badge="All Channels")
    try:
        from command_intel import comm_tracking_dashboard
        comm_tracking_dashboard.render()
    except Exception as _e:
        st.error(f"Communication Tracking failed to load: {_e}")

elif selected_page == "📓 Daily Log":
    _render_page_header("📓 Daily Log", "Sales & CRM", badge="Team Notes")
    try:
        cmd_daily_log.render()
    except Exception as _e:
        st.error(f"Daily Log failed to load: {_e}")

elif selected_page == "🚨 Alert Center":
    _render_page_header("🚨 Alert Center", "System", badge="P0/P1/P2")
    try:
        cmd_alert_center.render()
    except Exception as _e:
        st.error(f"Alert Center failed to load: {_e}")

elif selected_page == "🏥 Health Monitor":
    _render_page_header("🏥 Health Monitor", "System", badge="Live")
    try:
        from command_intel import health_monitor_dashboard
        health_monitor_dashboard.render()
    except Exception as _e:
        st.error(f"Health Monitor failed to load: {_e}")

elif selected_page == "🎛️ System Control Center":
    _render_page_header("🎛️ System Control Center", "System", badge="Live")
    try:
        from command_intel import system_control_center
        system_control_center.render()
    except Exception as _e:
        st.error(f"System Control Center failed to load: {_e}")

elif selected_page == "🤖 AI Setup & Workers":
    _render_page_header("🤖 AI Setup & Workers", "System", badge="Live")
    try:
        from command_intel import ai_setup_dashboard
        ai_setup_dashboard.render()
    except Exception as _e:
        st.error(f"AI Setup & Workers failed to load: {_e}")

elif selected_page == "🖥️ Ops Dashboard":
    _render_page_header("🖥️ Ops Dashboard", "System", badge="Live")
    try:
        from command_intel import ops_dashboard
        ops_dashboard.render()
    except Exception as _e:
        st.error(f"Ops Dashboard failed to load: {_e}")

elif selected_page == "📦 System Requirements":
    _render_page_header("📦 System Requirements", "System", badge="Info")
    try:
        from command_intel import system_requirements_dashboard
        system_requirements_dashboard.render()
    except Exception as _e:
        st.error(f"System Requirements failed to load: {_e}")

elif selected_page == "🤖 AI Learning":
    _render_page_header("🤖 AI Learning", "AI & Knowledge", badge="Continuous")
    st.info("AI learning engine — daily/weekly/monthly cycles that improve prediction accuracy over time.")
    try:
        from ai_learning_engine import AILearningEngine

        _ai_eng = AILearningEngine()
        _ai_tabs = st.tabs(["Model Status", "Run Learning", "Learning History", "Weights"])

        with _ai_tabs[0]:
            st.subheader("Model Accuracy")
            _acc = _ai_eng.get_model_accuracy()
            _ac1, _ac2, _ac3 = st.columns(3)
            _ac1.metric("Price Accuracy (7d)", f"{_acc.get('price_7d', 50)}%")
            _ac2.metric("Total Learning Cycles", _acc.get("total_cycles", 0))
            _ac3.metric("Last Updated", _acc.get("last_updated", "Never"))

        with _ai_tabs[1]:
            st.subheader("Manual Learning Trigger")
            _lc1, _lc2, _lc3 = st.columns(3)
            with _lc1:
                if st.button("Run Daily Learn", type="primary", use_container_width=True, key="ai_daily"):
                    _result = _ai_eng.daily_learn()
                    st.json(_result)
            with _lc2:
                if st.button("Run Weekly Learn", use_container_width=True, key="ai_weekly"):
                    _result = _ai_eng.weekly_learn()
                    st.json(_result)
            with _lc3:
                if st.button("Run Monthly Learn", use_container_width=True, key="ai_monthly"):
                    _result = _ai_eng.monthly_learn()
                    st.json(_result)

        with _ai_tabs[2]:
            st.subheader("Learning History")
            _hist = _ai_eng.get_learning_history(limit=30)
            if _hist:
                _hist_df = pd.DataFrame(_hist)
                st.dataframe(_hist_df, use_container_width=True, hide_index=True)
            else:
                st.caption("No learning cycles recorded yet. Run a sync or trigger manually.")

        with _ai_tabs[3]:
            st.subheader("Learned Factor Weights")
            _weights = _ai_eng.get_learned_weights()
            for _wk, _wv in _weights.items():
                st.progress(min(1.0, _wv), text=f"{_wk}: {_wv:.3f}")

    except Exception as _e:
        st.error(f"AI Learning failed to load: {_e}")

elif selected_page == "🏗️ Infra Demand Intelligence":
    _render_page_header("🏗️ Infra Demand Intelligence", "Intelligence", badge="GDELT + Budget")
    try:
        cmd_infra_demand.render()
    except Exception as _e:
        st.error(f"Infra Demand Intelligence failed to load: {_e}")

elif selected_page == "📡 Market Signals":
    _render_page_header("📡 Market Signals", "Intelligence", badge="AI Signal")
    st.info("ℹ️ **10-Signal Composite Intelligence.** Crude (25%) + Currency (15%) + News (15%) + Weather (10%) + Govt (10%) + Tenders (10%) + Economic (5%) + Search (5%) + Ports (5%) = Master Signal. Refreshes every 2 hours.")
    try:
        cmd_market_signals.render()
    except Exception as _e:
        st.error(f"⚠️ Market Signals failed to load: {_e}")
        st.info("Try reloading the page.")

elif selected_page == "🛠️ Developer Ops Map":
    _render_page_header("🛠️ Developer Ops Map", "Developer", badge="Ops")
    try:
        cmd_dev_ops.render()
    except Exception as _e:
        st.error(f"⚠️ Developer Ops Map failed to load: {_e}")
        st.info("Try reloading the page.")

elif selected_page == "🗺️ Dashboard Flow Map":
    _render_page_header("🗺️ Dashboard Flow Map", "Developer", badge="Blueprint")
    st.info("ℹ️ **Master System Blueprint.** Visual architecture of the entire dashboard — data sources, AI processing, calculations, storage, visualization, reports, and alerts.")
    try:
        cmd_flow_map.render()
    except Exception as _e:
        st.error(f"⚠️ Dashboard Flow Map failed to load: {_e}")
        st.info("Try reloading the page.")


# ══════════════════════════════════════════════════════════════════════════════
# GLOBAL FOOTER — PPS Anantam Agentic AI Eco System
# ══════════════════════════════════════════════════════════════════════════════
st.markdown("""
<div style="
  background: #f0ebe1;
  border-top: 1px solid #e8dcc8;
  padding: 10px 24px;
  margin-top: 28px;
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  justify-content: space-between;
  gap: 6px;
  font-family: 'Inter', 'Segoe UI', sans-serif;
">
  <div>
    <span style="font-size:0.88rem;font-weight:800;color:#1e3a5f;">
      🏛️ PPS Anantam Agentic AI Eco System
    </span>
    <span style="font-size:0.72rem;color:#475569;margin-left:10px;">
      Vadodara, Gujarat
    </span>
  </div>
  <div style="font-size:0.73rem;color:#475569;display:flex;gap:14px;flex-wrap:wrap;">
    <span>Version v4.0.0</span>
    <span>Build: 03-03-2026</span>
    <span>Environment: Production</span>
    <span style="color:#c9a84c;font-weight:600;">GST: 24AAHCV1611L2ZD</span>
  </div>
</div>
""", unsafe_allow_html=True)
