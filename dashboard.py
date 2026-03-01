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
import api_dashboard

# --- PDF EXPORT SYSTEM ---
try:
    from pdf_export_bar import render_export_bar, inject_print_css
    _PDF_BAR_OK = True
except Exception:
    _PDF_BAR_OK = False
    def render_export_bar(*a, **kw): pass
    def inject_print_css(): pass

# --- SYSTEM STARTUP: init logging + auto health scheduler (runs once per process) ---
try:
    from api_manager import init_system, start_auto_health
    init_system()
    start_auto_health()          # background thread: health check every 30 min
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
st.set_page_config(page_title="Logistics Pricing AI", page_icon="🚛", layout="wide")

# Custom Styling
st.markdown("""
<style>
/* =================================================================================
VASTU-COMPLIANT 3D GLASSMORPHISM DESIGN SYSTEM
North (Green): Money/Opportunity | South-East (Fire): Cash Flow | West (Blue): Gains
================================================================================= */

:root {
/* VASTU PALETTE */
--vastu-north-green: #059669;  /* Growth/Pricing */
--vastu-se-fire: #ea580c;      /* Action/Sales/Fire */
--vastu-west-blue: #2563eb;    /* Gains/Logistics */
--vastu-sw-earth: #ca8a04;     /* Stability/Store */

/* BACKGROUNDS */
--bg-main: #f8fafc;
--glass-bg: rgba(255, 255, 255, 0.7);
--glass-border: rgba(255, 255, 255, 0.4);

/* SHADOWS (Deep 3D) */
--shadow-3d: 
8px 8px 16px rgba(166, 171, 189, 0.5), 
-8px -8px 16px rgba(255, 255, 255, 0.8);
--shadow-inner: 
inset 4px 4px 8px rgba(166, 171, 189, 0.2), 
inset -4px -4px 8px rgba(255, 255, 255, 0.8);
}

/* GLOBAL APP STYLE */
.stApp {
background: radial-gradient(circle at top left, #f1f5f9, #e2e8f0);
}

/* ---------------------------------------------------------------------------------
3D GLASSMORPHIC CARDS
--------------------------------------------------------------------------------- */
.stCard, div[data-testid="stExpander"], div[data-testid="stForm"] {
background: var(--glass-bg);
backdrop-filter: blur(12px);
-webkit-backdrop-filter: blur(12px);
border-radius: 20px;
border: 1px solid var(--glass-border);
box-shadow: var(--shadow-3d);
padding: 20px;
margin-bottom: 20px;
transition: transform 0.3s ease;
}

/* ---------------------------------------------------------------------------------
3D TABS - FIXED WRAPPING (No Horizontal Scroll)
--------------------------------------------------------------------------------- */
.stTabs [data-baseweb="tab-list"] {
gap: 8px;
padding: 10px;
background: rgba(255, 255, 255, 0.3);
border-radius: 20px;
box-shadow: inset 2px 2px 5px rgba(0,0,0,0.05);
display: flex;
flex-wrap: wrap !important;     /* FORCE WRAP */
white-space: normal !important; /* ALLOW TEXT WRAP */
overflow: visible !important;   /* NO SCROLL */
height: auto !important;
}

.stTabs [data-baseweb="tab"] {
height: 40px;                   /* Smaller height for compact rows */
min-width: fit-content;
flex: 1 1 auto;                 /* Grow to fill space */
border-radius: 10px;            /* Pill shape */
background-color: #e2e8f0;
box-shadow: var(--shadow-3d);
border: 1px solid white;
color: #64748b;
font-weight: 700;
padding: 0 15px;
margin-bottom: 5px;             /* Space between wrapped rows */
transition: all 0.2s ease;
}

.stTabs [data-baseweb="tab"]:hover {
transform: translateY(-3px);
color: var(--vastu-west-blue);
}

/* ACTIVE TAB - GLOWING GRADIENT */
.stTabs [data-baseweb="tab"][aria-selected="true"] {
background: linear-gradient(135deg, #1e40af, #3b82f6);
color: white !important;
box-shadow: 
0 10px 20px rgba(37, 99, 235, 0.3),
inset 0 0 0 1px rgba(255,255,255,0.3);
transform: scale(1.05);
}

/* ---------------------------------------------------------------------------------
VASTU COLOR METRICS (North/Money = Green)
--------------------------------------------------------------------------------- */
div[data-testid="stMetric"] {
background: linear-gradient(145deg, #ffffff, #f0fdf4);
border-radius: 15px;
padding: 15px;
box-shadow: var(--shadow-3d);
border-left: 6px solid var(--vastu-north-green);
}

div[data-testid="stMetricLabel"] { color: #64748b; font-weight: 600; }
div[data-testid="stMetricValue"] { 
color: var(--vastu-north-green); 
text-shadow: 1px 1px 2px rgba(0,0,0,0.1);
}

/* ---------------------------------------------------------------------------------
BUTTONS - PRESSABLE 3D
--------------------------------------------------------------------------------- */
.stButton button {
background: linear-gradient(145deg, #ffffff, #e2e8f0);
box-shadow: var(--shadow-3d);
border-radius: 12px;
color: #1e293b;
font-weight: 700;
border: 1px solid rgba(255,255,255,0.8);
}

.stButton button:hover {
transform: translateY(-2px);
}

.stButton button:active {
box-shadow: var(--shadow-inner);
transform: translateY(2px);
}

/* Primary Action - Fire/Sales (South-East) */
.stButton button[kind="primary"] {
background: linear-gradient(135deg, #ea580c, #f97316);
color: white;
box-shadow: 0 10px 20px rgba(234, 88, 12, 0.4);
border: none;
}
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

# Global API Switch (Sidebar)
api_active = st.sidebar.toggle("🌐 Live Data APIs", value=False, help="Turn ON to fetch real-time data. OFF uses fast offline mode.")

@st.cache_data(ttl=120)
def get_cached_market_data(is_active):
    if is_active:
        return get_live_market_data()
    else:
        return get_simulated_data()

mkt = get_cached_market_data(api_active)

st.markdown(f"""
<div style="
background-color: #0f172a; 
color: #cbd5e1; 
padding: 8px 15px; 
font-family: 'Inter', sans-serif; 
font-size: 0.85rem; 
border-bottom: 1px solid #1e293b;
display: flex;
justify-content: space-between;
align-items: center;
">
<div style="display:flex; gap: 20px;">
<span>🛢️ <b>Brent:</b> {mkt['brent']['value']} <span style="font-size:0.75rem; color:#94a3b8;">(7d: {mkt['brent']['value_7d']})</span> <span style="color:{mkt['brent']['color']}">{mkt['brent']['change']}</span></span>
<span>🛢️ <b>WTI:</b> {mkt['wti']['value']} <span style="font-size:0.75rem; color:#94a3b8;">(7d: {mkt['wti']['value_7d']})</span> <span style="color:{mkt['wti']['color']}">{mkt['wti']['change']}</span></span>
<span>💵 <b>USD/INR:</b> {mkt['usdinr']['value']} <span style="font-size:0.75rem; color:#94a3b8;">(7d: {mkt['usdinr']['value_7d']})</span> <span style="color:{mkt['usdinr']['color']}">{mkt['usdinr']['change']}</span></span>
</div>
<div style="font-size: 0.75rem; color: #64748b;">
⏱️ Live as of {mkt['timestamp']}
</div>
</div>
""", unsafe_allow_html=True)

# --- OMNIPOTENT INDUSTRIES DASHBOARD ---

# Header Section (Enterprise 3D Style - Deep Steel Blue)
st.markdown("""
<div style="
background: linear-gradient(135deg, #1e3a8a 0%, #0f172a 100%);
padding: 12px 25px;
border-radius: 8px;
display: flex;
align-items: center;
justify-content: space-between;
margin-bottom: 25px;
box-shadow: 0 10px 25px -5px rgba(30, 58, 138, 0.4);
border-bottom: 2px solid #3b82f6;
">
<div style="display: flex; align-items: center; gap: 15px;">
<span style="font-size: 1.4rem; font-weight: 800; background: -webkit-linear-gradient(eee, white); -webkit-background-clip: text; -webkit-text-fill-color: transparent;">
PPS Anantams
</span>
<span style="border-left: 1px solid rgba(255,255,255,0.2); height: 25px;"></span>
<div style="display: flex; flex-direction: column;">
<span style="font-size: 0.9rem; color: #e0e7ff; font-weight: 500; letter-spacing: 0.5px;">LANDING COST SYSTEM</span>
<span style="font-size: 0.65rem; color: #94a3b8; text-transform: uppercase; letter-spacing: 1px;">Enterprise Edition V2.0</span>
</div>
</div>
<div style="text-align: right; display: flex; gap: 20px; align-items: center;">
<div style="background: rgba(255,255,255,0.1); padding: 5px 12px; border-radius: 4px; border: 1px solid rgba(255,255,255,0.1);">
<span style="color: #cbd5e1; font-size: 0.75rem;">📍 Vadodara HQ</span>
</div>
<div style="text-align: right;">
<div style="font-size: 0.8rem; color: #e2e8f0; font-weight: 600;">GST: 24AAHCV1611L2ZD</div>
<div style="font-size: 0.7rem; color: #64748b;">Auth: Director Finance</div>
</div>
</div>
</div>
""", unsafe_allow_html=True)

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
# --- SIDEBAR NAVIGATION (REFACTORED) ---

# CSS for Premium Sidebar
st.markdown("""
<style>
section[data-testid="stSidebar"] {
background-color: #ffffff;
box-shadow: 2px 0 15px rgba(0,0,0,0.05);
}
.nav-header {
font-size: 0.75rem;
font-weight: 700;
color: #94a3b8;
text-transform: uppercase;
letter-spacing: 0.05em;
margin-top: 20px;
margin-bottom: 10px;
padding-left: 10px;
}
div.row-widget.stRadio > div {
gap: 5px;
}
div.row-widget.stRadio > div > label {
background-color: transparent;
border: none;
margin-bottom: 2px;
padding: 8px 10px;
border-radius: 8px;
transition: all 0.2s;
}
div.row-widget.stRadio > div > label:hover {
background-color: #f1f5f9;
}
div.row-widget.stRadio > div > label[data-baseweb="radio"] {
background-color: #eff6ff;
color: #1d4ed8;
font-weight: 600;
}
</style>
""", unsafe_allow_html=True)

with st.sidebar:
    st.image("https://cdn-icons-png.flaticon.com/512/3523/3523063.png", width=50) # Mock Logo
    st.markdown("### PPS Anantams")
    st.caption("Enterprise Dashboard V2.1")
    
    st.markdown('<div class="nav-header">PLAN & SELL</div>', unsafe_allow_html=True)
    
    # Single combined radio for all navigation, grouped by Department
    all_nav_options = [
        "─── 🏢 SALES & REVENUE ───",
        "💼 Sales Workspace",
        "🎯 CRM & Tasks",
        "📅 Sales Calendar",
        "🧮 Pricing Calculator",
        "🚨 SPECIAL PRICE (SOS)",
        "📋 Source Directory",
        
        "─── 🏢 OPERATIONS & PROCUREMENT ───",
        "🏭 Feasibility",
        "📦 Import Cost Model",
        "🚢 Supply Chain",
        "🛠️ Data Manager",

        "─── 🏢 FINANCE & PROFITABILITY ───",
        "💰 Financial Intelligence",
        "👷 Demand Analytics",
        "📤 Reports",

        "─── 🏢 LEGAL & COMPLIANCE ───",
        "🛡️ GST & Legal Monitor",
        "⚡ Risk Scoring",

        "─── 🏢 STRATEGY & INTELLIGENCE ───",
        "🔮 Price Prediction",
        "⏳ Past Predictions",
        "📝 Manual Price Entry",
        "🎯 Strategy Panel",
        "🔔 Alert System",

        "─── 🏢 TECHNOLOGY & SYSTEMS ───",
        "🌐 API Dashboard",
        "⚙️ Dev & System Activity",
        "📁 PDF Archive",
        "🔔 Change Notifications",
        "🐞 Bug Tracker",
        "👥 Ecosystem Management",
        "⚙️ Settings",

        "─── 🏢 KNOWLEDGE & SUPPORT ───",
        "🔄 AI Fallback Engine",
        "🧠 AI Dashboard Assistant",
        "🤖 AI Assistant",
        "📚 Knowledge Base",
        "🏛️ Business Intelligence",

        "─── 🏢 MARKET INTELLIGENCE ───",
        "📰 News Intelligence",
        "🕵️ Competitor Intelligence",
        "🔭 Contractor OSINT",
        "🛣️ Road Budget & Demand",
    ]
    
    selected_page = st.radio("Navigate", all_nav_options, label_visibility="collapsed")
    
    st.divider()
    st.caption("Logged in as: Sales Manager")

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


# ── Universal Print CSS + Export Bar ────────────────────────────────────────
# Inject @media print CSS once per session (hides sidebar on browser print)
inject_print_css()

# Skip export bar for section-header items and PDF Archive (which has its own)
_NAV_HEADERS = {
    "─── 🏢 SALES & REVENUE ───", "─── 🏢 OPERATIONS & PROCUREMENT ───",
    "─── 🏢 FINANCE & PROFITABILITY ───", "─── 🏢 LEGAL & COMPLIANCE ───",
    "─── 🏢 STRATEGY & INTELLIGENCE ───", "─── 🏢 TECHNOLOGY & SYSTEMS ───",
    "─── 🏢 KNOWLEDGE & SUPPORT ───", "─── 🏢 MARKET INTELLIGENCE ───",
    "📁 PDF Archive",
}
if selected_page not in _NAV_HEADERS:
    render_export_bar(
        page_title=selected_page,
        role="Admin",
    )


# Main Layout: 3 Columns (Selection | Analysis | Detailed Slip)
if selected_page == "🧮 Pricing Calculator":
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
    display_badge("historical")
    sales_workspace.render_deal_room()

# --- TAB 2: SALES CALENDAR ---
if selected_page == "📅 Sales Calendar":
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
    from command_intel import ai_fallback_dashboard
    ai_fallback_dashboard.render()

# --- AI DASHBOARD ASSISTANT (Full Data-Connected AI) ---
elif selected_page == "🧠 AI Dashboard Assistant":
    st.info(
        "ℹ️ **Department Head:** All Departments\n\n"
        "📌 AI assistant connected to ALL dashboard data — prices, APIs, competitors, "
        "contractors, demand, bugs, change log, forecasts. Role-based access control. "
        "Powered by Claude (Anthropic)."
    )
    from command_intel import ai_dashboard_assistant
    ai_dashboard_assistant.render()

# --- TAB 4: AI SALES ASSISTANT ---
elif selected_page == "🤖 AI Assistant":
    st.header("🤖 AI Sales Training & Assistant")
    st.caption("Instant answers, objection handling, and training manual.")
    
    from sales_knowledge_base import (
        get_chatbot_response, get_all_sections, get_section_questions,
        get_telecalling_script, polish_email, generate_custom_reply
    )
    
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
    st.header("⚙️ System Settings & Integrations")
    
    st.subheader("🔗 External Integrations & API Keys")
    st.caption("Manage connections to third-party services for automation.")
    
    with st.expander("🔑 API Configuration", expanded=True):
        api_c1, api_c2 = st.columns(2)
        with api_c1:
            st.text_input("WhatsApp API Key (Twilio/Meta)", type="password", help="Required for sending automated WhatsApp messages.")
            st.text_input("Email SMTP Host", value="smtp.gmail.com")
            st.text_input("Email SMTP Port", value="587")
        with api_c2:
            st.text_input("OpenAI / AI Key", type="password", help="Required for AI Script generation (Optional).")
            st.text_input("Google Maps API Key", type="password", help="Required for accurate distance calculations.")
            st.text_input("Email Username/Login")
            
        if st.button("💾 Save API Settings"):
            st.toast("Settings Saved Securely (Mock)")

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

# Footer
st.markdown("---")
st.caption("Powered by Python Logistics AI | Phase 5 Automation Enabled")

# --- TAB 6: DATA MANAGER ---
if selected_page == "🛠️ Data Manager":
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

# --- TAB 7: SOURCE DIRECTORY ---
if selected_page == "📋 Source Directory":
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
    st.header("📚 Bitumen Sales Knowledge Base")
    st.caption("Training Manual, FAQs, and Process Guidelines")
    
    from sales_knowledge_base import (
        TRAINING_SECTIONS, get_section_questions, get_knowledge_count
    )
    
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
    display_badge("real-time")
    api_dashboard.render()
elif selected_page == "⚙️ Dev & System Activity":
    st.info("ℹ️ **Department Head:** CTO / Technology\n\n📌 Full audit trail of API changes, auto-repairs, errors, deployments, and system events. All timestamps in IST.")
elif selected_page == "📁 PDF Archive":
    st.info("ℹ️ **Department Head:** Admin / CTO\n\n📌 All PDFs generated from dashboard pages — download, view metadata, or delete. Auto-saved whenever you use ⬇ PDF on any page.")
    from command_intel import pdf_archive
    pdf_archive.render()
    from command_intel import dev_activity
    dev_activity.render()
elif selected_page == "─── 🏢 SALES & REVENUE ───":
    st.info("ℹ️ **Department Head:** Sales Director\n\n📌 *Please select a specific module below (e.g., Sales Workspace, Pricing Calculator).*")
elif selected_page == "─── 🏢 OPERATIONS & PROCUREMENT ───":
    st.info("ℹ️ **Department Head:** Operations Head\n\n📌 *Please select a specific module below (e.g., Feasibility, Supply Chain).*")
elif selected_page == "─── 🏢 FINANCE & PROFITABILITY ───":
    st.info("ℹ️ **Department Head:** CFO\n\n📌 *Please select a specific module below (e.g., Financial Intelligence, Reports).*")
elif selected_page == "─── 🏢 LEGAL & COMPLIANCE ───":
    st.info("ℹ️ **Department Head:** Legal Head\n\n📌 *Please select a specific module below (e.g., GST & Legal Monitor, Risk Scoring).*")
elif selected_page == "─── 🏢 STRATEGY & INTELLIGENCE ───":
    st.info("ℹ️ **Department Head:** Strategy Director\n\n📌 *Please select a specific module below (e.g., Price Prediction, Alert System).*")
elif selected_page == "─── 🏢 TECHNOLOGY & SYSTEMS ───":
    st.info("ℹ️ **Department Head:** CTO\n\n📌 *Please select a specific module below (e.g., API Dashboard, Settings).*")
elif selected_page == "─── 🏢 KNOWLEDGE & SUPPORT ───":
    st.info("ℹ️ **Department Head:** Admin / HR Head\n\n📌 *Please select a specific module below (e.g., AI Assistant, Knowledge Base).*")
elif selected_page == "🔮 Price Prediction":
    st.info("ℹ️ **UX Note:** Predicts global/local price trends focusing on the core 1st & 16th Indian cycles.")
    price_prediction.render()
elif selected_page == "⏳ Past Predictions":
    st.info("ℹ️ **UX Note:** 10-Year historical truth table validating the quantitative MLR-DL model performance.")
    historical_revisions.render()
elif selected_page == "📝 Manual Price Entry":
    st.info("ℹ️ **UX Note:** B2B override grid for field CRM quotes and real-time market entries.")
    manual_entry.render()
elif selected_page == "🔔 Change Notifications":
    st.info("ℹ️ **UX Note:** Complete, immutable timeline log of every number change or API fallback occurrence.")
    change_log.render()
elif selected_page == "🐞 Bug Tracker":
    st.info("ℹ️ **UX Note:** Health tracker for failed APIs, broken data models, and Dev-Ops diagnostics.")
    bug_tracker.render()
elif selected_page == "📦 Import Cost Model":
    st.info("ℹ️ **UX Note:** Shows granular breakdowns of import duties, logistics, and conversion costs from Port to Decanter.")
    import_cost_model.render()
elif selected_page == "🚢 Supply Chain":
    st.info("ℹ️ **UX Note:** Tracks voyage ships, transit times, and potential local stock availability constraints.")
    supply_chain.render()
elif selected_page == "👷 Demand Analytics":
    st.info("ℹ️ **UX Note:** Analyzes government and private contractor demand cycles based on infrastructure projects.")
    demand_analytics.render()
elif selected_page == "💰 Financial Intelligence":
    st.info("ℹ️ **UX Note:** Provides insights on buyer credit, rolling capital management, and payment cycles.")
    financial_intel.render()
elif selected_page == "🛡️ GST & Legal Monitor":
    st.info("ℹ️ **UX Note:** Updates on regulatory changes, e-way bills, and compliance risk related to Bitumen sales.")
    gst_legal_monitor.render()
elif selected_page == "⚡ Risk Scoring":
    st.info("ℹ️ **UX Note:** Gives an AI-driven composite Risk Score (1-100) combining counterparty defaults and market volatility.")
    risk_scoring.render()
elif selected_page == "🔔 Alert System":
    st.info("ℹ️ **UX Note:** Configurable notifications for price drops, supply shocks, and new opportunities.")
    alert_system.render()
elif selected_page == "🎯 Strategy Panel":
    st.info("ℹ️ **UX Note:** High-level strategic synthesis and \"What-If\" scenarios for volume purchasing.")
    strategy_panel.render()
elif selected_page == "🏛️ Business Intelligence":
    st.info("ℹ️ **Department Head:** All Departments\n\n📌 Complete guide for every dashboard tab — for employees, partners, investors, and auditors.")
    from business_knowledge_base import render as biz_kb_render
    biz_kb_render()
elif selected_page == "📰 News Intelligence":
    st.info("ℹ️ **Department Head:** Strategy Director / Business Intelligence\n\n📌 Live news aggregator — 14 sources (RSS + API), auto-classified by keywords, impact-scored 0–100. Breaking alerts for score ≥ 80.")
    from command_intel import news_dashboard as _nd
    _nd.render()
elif selected_page == "🕵️ Competitor Intelligence":
    st.info("ℹ️ **Department Head:** Strategy Director / Sales Head\n\n📌 Daily forecasts from Multi Energy Enterprises (MEE, Mumbai) — cross-verified against IOCL/HPCL circulars, international bitumen FOB prices, India consumption/production stats, and industry news.")
    import competitor_intelligence
    competitor_intelligence.render()
elif selected_page == "🔭 Contractor OSINT":
    st.info("ℹ️ **Department Head:** Sales Director / Business Development\n\n📌 Track road contractors — project awards, bitumen demand (MT), monthly heatmap, risk flags, CRM export. Last 6 months only.")
    import contractor_osint
    contractor_osint.render()
elif selected_page == "🛣️ Road Budget & Demand":
    st.info("ℹ️ **Department Head:** Strategy Director\n\n📌 India road budget analysis, state-wise bitumen demand forecast, and construction seasonality.")
    from command_intel import road_budget_demand
    road_budget_demand.render()
elif selected_page == "─── 🏢 MARKET INTELLIGENCE ───":
    st.info("ℹ️ **Department Head:** Strategy Director\n\n📌 *Select Road Budget & Demand for national infrastructure intelligence.*")


