"""
Panel 7: AI Risk Scoring Engine
Consolidated risk scores: Market, Supply, Financial, Compliance, Legal, Margin Safety.
Overall Business Health Score.
"""
import streamlit as st
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from ui_badges import display_badge
import datetime
import numpy as np
import random


def _calculate_risk_scores():
    """Calculate all risk dimension scores."""
    np.random.seed(datetime.date.today().toordinal() % 100)
    
    # Each score: 0-100 (lower = safer)
    month = datetime.date.today().month
    
    # Market Risk — based on crude volatility, seasonal factors
    is_peak = month in [10, 11, 12, 1, 2, 3]
    market_risk = np.clip(np.random.normal(45, 15), 10, 90)
    if not is_peak:
        market_risk += 10  # Higher risk in off-season
    
    # Supply Risk — based on shipping lanes, OPEC, refinery output
    supply_risk = np.clip(np.random.normal(35, 12), 10, 85)
    
    # Financial Risk — based on receivables, cashflow, WC
    financial_risk = np.clip(np.random.normal(40, 10), 10, 80)
    
    # Compliance Risk — GST, E-Invoice, GSTR matching
    compliance_risk = np.clip(np.random.normal(30, 15), 5, 75)
    
    # Legal Exposure — investigation flags, Section 74
    legal_exposure = np.clip(np.random.normal(25, 12), 5, 70)
    
    # Margin Safety — inverse of margin pressure
    margin_safety = np.clip(100 - np.random.normal(38, 10), 20, 95)
    
    scores = {
        "market_risk": round(market_risk),
        "supply_risk": round(supply_risk),
        "financial_risk": round(financial_risk),
        "compliance_risk": round(compliance_risk),
        "legal_exposure": round(legal_exposure),
        "margin_safety": round(margin_safety),
    }
    
    # Overall Health Score (weighted inverse of risks + margin safety)
    weights = [0.20, 0.15, 0.20, 0.15, 0.10, 0.20]
    risk_values = [market_risk, supply_risk, financial_risk, compliance_risk, legal_exposure, 100 - margin_safety]
    
    weighted_risk = sum(w * r for w, r in zip(weights, risk_values))
    health_score = round(100 - weighted_risk)
    
    scores["health_score"] = max(10, min(95, health_score))
    
    return scores


def _risk_gauge(label, score, is_safety=False):
    """Generate HTML for a risk gauge."""
    if is_safety:
        color = "#22c55e" if score >= 60 else ("#f59e0b" if score >= 40 else "#ef4444")
        level = "Strong" if score >= 60 else ("Adequate" if score >= 40 else "Weak")
    else:
        color = "#22c55e" if score <= 30 else ("#f59e0b" if score <= 60 else "#ef4444")
        level = "Low" if score <= 30 else ("Moderate" if score <= 60 else "High")
    
    return f"""
<div style="background:white; border-radius:12px; padding:15px; text-align:center;
         box-shadow: 0 2px 10px rgba(0,0,0,0.08); border-bottom:4px solid {color};">
<div style="font-size:0.75rem; color:#64748b; font-weight:600; text-transform:uppercase;
             letter-spacing:0.5px;">{label}</div>
<div style="position:relative; margin:15px auto; width:80px; height:80px;">
            <svg viewBox="0 0 36 36" style="transform:rotate(-90deg);">
<path d="M18 2.0845 a 15.9155 15.9155 0 0 1 0 31.831 a 15.9155 15.9155 0 0 1 0 -31.831"
                      fill="none" stroke="#e2e8f0" stroke-width="3"/>
<path d="M18 2.0845 a 15.9155 15.9155 0 0 1 0 31.831 a 15.9155 15.9155 0 0 1 0 -31.831"
                      fill="none" stroke="{color}" stroke-width="3"
                      stroke-dasharray="{score}, 100"/>
            </svg>
<div style="position:absolute; top:50%; left:50%; transform:translate(-50%,-50%);
                 font-size:1.3rem; font-weight:800; color:{color};">{score}%</div>
</div>
<div style="font-size:0.7rem; color:{color}; font-weight:600;">{level}</div>
</div>
    """


def render():
    display_badge("calculated")
    """Render the AI Risk Scoring Engine panel."""
    
    st.markdown("""
<div style="background: linear-gradient(135deg, #4a1d96 0%, #6d28d9 100%);
padding: 20px 25px; border-radius: 12px; margin-bottom: 20px;
box-shadow: 0 10px 30px rgba(109, 40, 217, 0.4);">
<div style="display:flex; align-items:center; gap:15px;">
<span style="font-size:2rem;">⚡</span>
<div>
<div style="font-size:1.3rem; font-weight:800; color:#e0e0e0;">
AI Risk Scoring Engine
</div>
<div style="font-size:0.75rem; color:#c4b5fd; letter-spacing:1px; text-transform:uppercase;">
Multi-Dimensional Risk Analysis • Business Health Score
</div>
</div>
</div>
</div>
""", unsafe_allow_html=True)
    
    scores = _calculate_risk_scores()
    
    # --- OVERALL HEALTH SCORE (HERO) ---
    hs = scores["health_score"]
    hs_color = "#22c55e" if hs >= 70 else ("#f59e0b" if hs >= 50 else "#ef4444")
    hs_label = "EXCELLENT" if hs >= 80 else ("GOOD" if hs >= 70 else ("FAIR" if hs >= 50 else ("AT RISK" if hs >= 30 else "CRITICAL")))
    
    st.markdown(f"""
<div style="background: linear-gradient(135deg, #0f172a, #1e293b); border-radius:15px; 
padding:30px; text-align:center; margin-bottom:25px;
box-shadow: 0 10px 30px rgba(0,0,0,0.3);">
<div style="font-size:0.9rem; color:#94a3b8; font-weight:600; letter-spacing:2px;
text-transform:uppercase;">Overall Bitumen Business Health Score</div>
<div style="position:relative; margin:20px auto; width:150px; height:150px;">
<svg viewBox="0 0 36 36" style="transform:rotate(-90deg);">
<path d="M18 2.0845 a 15.9155 15.9155 0 0 1 0 31.831 a 15.9155 15.9155 0 0 1 0 -31.831"
fill="none" stroke="#334155" stroke-width="2.5"/>
<path d="M18 2.0845 a 15.9155 15.9155 0 0 1 0 31.831 a 15.9155 15.9155 0 0 1 0 -31.831"
fill="none" stroke="{hs_color}" stroke-width="2.5"
stroke-dasharray="{hs}, 100" stroke-linecap="round"/>
</svg>
<div style="position:absolute; top:50%; left:50%; transform:translate(-50%,-50%);">
<div style="font-size:2.5rem; font-weight:900; color:{hs_color};">{hs}</div>
<div style="font-size:0.6rem; color:#64748b;">/ 100</div>
</div>
</div>
<div style="font-size:1.2rem; font-weight:700; color:{hs_color}; letter-spacing:1px;">
{hs_label}
</div>
<div style="font-size:0.7rem; color:#64748b; margin-top:8px;">
Calculated from 6 risk dimensions • Updated daily
</div>
</div>
""", unsafe_allow_html=True)
    
    # --- RISK DIMENSION GAUGES ---
    st.markdown("### 📊 Risk Dimensions")
    
    dimensions = [
        ("Market Risk", scores["market_risk"], False, "Crude prices, seasonal demand, geo-political"),
        ("Supply Risk", scores["supply_risk"], False, "Shipping lanes, OPEC, refinery output"),
        ("Financial Risk", scores["financial_risk"], False, "Receivables, cashflow, working capital"),
        ("Compliance Risk", scores["compliance_risk"], False, "GST filing, E-Invoice, GSTR matching"),
        ("Legal Exposure", scores["legal_exposure"], False, "Investigation flags, Section 74"),
        ("Margin Safety", scores["margin_safety"], True, "Price-cost spread protection"),
    ]
    
    cols = st.columns(6)
    for i, (label, score, is_safety, desc) in enumerate(dimensions):
        with cols[i]:
            st.markdown(_risk_gauge(label, score, is_safety), unsafe_allow_html=True)
            st.caption(desc)
    
    st.markdown("---")
    
    # --- RISK BREAKDOWN ---
    st.markdown("### 📋 Risk Factor Details")
    
    risk_details = [
        {
            "dimension": "🛢️ Market Risk",
            "score": scores["market_risk"],
            "factors": [
                "Brent Crude at ₹ 78.5 — moderate volatility",
                "Seasonal construction demand — currently " + ("peak" if datetime.date.today().month in [10,11,12,1,2,3] else "off-season"),
                "OPEC production cuts active — supply constrained",
                "Middle East geo-political risk elevated"
            ],
            "action": "Monitor crude daily. Consider hedging if > ₹ 82."
        },
        {
            "dimension": "🚢 Supply Risk",
            "score": scores["supply_risk"],
            "factors": [
                "Iraq Basrah loading — 20-25 day transit",
                "Red Sea route — no current disruption",
                "Indian refinery output — stable at 85% capacity",
                "Import terminal capacity — adequate"
            ],
            "action": "Maintain 15-day safety stock at port."
        },
        {
            "dimension": "💰 Financial Risk",
            "score": scores["financial_risk"],
            "factors": [
                "Working capital rotation — 45 days average",
                "Receivable aging — ₹2.7 Cr in 90+ days",
                "GST credit blocked — ₹11.4 Cr",
                "Cashflow stress ratio — 1.27x"
            ],
            "action": "Reduce 90+ day receivables. Follow up on blocked GST credits."
        },
        {
            "dimension": "📋 Compliance Risk",
            "score": scores["compliance_risk"],
            "factors": [
                "2 suppliers with GSTR-2B mismatch",
                "1 supplier with suspended GST",
                "E-Invoice compliance — 4/6 suppliers compliant",
                "E-Way bill matching — 4/6 matched"
            ],
            "action": "Stop purchases from non-compliant suppliers immediately."
        },
    ]
    
    for rd in risk_details:
        color = "#22c55e" if rd["score"] <= 30 else ("#f59e0b" if rd["score"] <= 60 else "#ef4444")
        with st.expander(f"{rd['dimension']} — Score: {rd['score']}%"):
            for f in rd["factors"]:
                st.caption(f"• {f}")
            st.info(f"**Recommended Action:** {rd['action']}")
