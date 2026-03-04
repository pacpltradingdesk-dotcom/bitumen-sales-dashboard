"""
PPS Anantam — Navigation Configuration v4.0
=============================================
Single source of truth for the enterprise navigation structure.
10 sidebar modules → 4 sub-tabs each → maps to existing page handlers.
"""
from __future__ import annotations

# ═══════════════════════════════════════════════════════════════════════════════
# MODULE NAVIGATION MAP
# Each module has max 4 sub-tabs.  "page" = the exact old page string used by
# the if/elif dispatch chain in dashboard.py.  "also" = extra pages accessible
# via a secondary switcher within that sub-tab.
# ═══════════════════════════════════════════════════════════════════════════════

MODULE_NAV: dict[str, dict] = {
    "🏠 Home": {
        "icon": "🏠", "label": "Home",
        "tabs": [
            {"label": "Live Market",    "page": "🏠 Home"},
            {"label": "Top Targets",    "page": "🔍 Opportunities"},
            {"label": "Live Alerts",    "page": "🚨 Alert Center"},
            {"label": "Quick Send",     "page": "_quick_send"},
        ],
    },
    "📌 Director Briefing": {
        "icon": "📌", "label": "Director Briefing",
        "tabs": [
            {"label": "Today Focus",       "page": "📋 Director Briefing"},
            {"label": "Sales Calendar",     "page": "📅 Sales Calendar"},
            {"label": "Alert System",       "page": "🔔 Alert System"},
            {"label": "Daily Log",          "page": "📓 Daily Log"},
        ],
    },
    "💰 Procurement": {
        "icon": "💰", "label": "Procurement",
        "tabs": [
            {"label": "Pricing Desk",       "page": "🧮 Pricing Calculator",
             "also": ["📝 Manual Price Entry", "🚨 SPECIAL PRICE (SOS)"]},
            {"label": "Supplier Directory",  "page": "📋 Source Directory"},
            {"label": "Import Costing",      "page": "📦 Import Cost Model",
             "also": ["🛒 Purchase Advisor"]},
            {"label": "Price Alerts",        "page": "🔮 Price Prediction"},
        ],
    },
    "🧾 Sales": {
        "icon": "🧾", "label": "Sales",
        "tabs": [
            {"label": "Buyer CRM",          "page": "🎯 CRM & Tasks",
             "also": ["💼 Sales Workspace"]},
            {"label": "Quotations & Deals",  "page": "🤝 Negotiation Assistant"},
            {"label": "Follow-ups",          "page": "💬 Communication Hub",
             "also": ["📓 Daily Log", "📊 Comm Tracking"]},
            {"label": "Channels Setup",      "page": "📧 Email Setup",
             "also": ["📱 WhatsApp Setup", "🔗 Integrations"]},
        ],
    },
    "🚚 Logistics": {
        "icon": "🚚", "label": "Logistics",
        "tabs": [
            {"label": "Maritime Intel",      "page": "🚢 Maritime Logistics"},
            {"label": "Freight Calculator",  "page": "🏭 Feasibility"},
            {"label": "Supply Chain",        "page": "🚢 Supply Chain",
             "also": ["⚓ Port Import Tracker"]},
            {"label": "Ecosystem",           "page": "👥 Ecosystem Management"},
        ],
    },
    "🧠 Intelligence": {
        "icon": "🧠", "label": "Intelligence",
        "tabs": [
            {"label": "Market Signals",      "page": "📡 Market Signals",
             "also": ["📰 News Intelligence", "🏗️ Infra Demand Intelligence",
                      "🔭 Contractor OSINT", "🗂️ India Procurement Directory"]},
            {"label": "Real-time Insights",  "page": "🔴 Real-time Insights",
             "also": ["🌐 Global Markets", "🏭 Refinery Supply"]},
            {"label": "Business Advisor",    "page": "🧑‍💼 Business Advisor",
             "also": ["📋 Discussion Guide"]},
            {"label": "Recommendations",     "page": "💡 Recommendations",
             "also": ["👷 Demand Analytics", "🔍 Opportunities",
                      "📈 Demand Correlation", "🕵️ Competitor Intelligence",
                      "🔮 Price Prediction"]},
        ],
    },
    "🛡 Compliance": {
        "icon": "🛡", "label": "Compliance",
        "tabs": [
            {"label": "Govt Data Hub",   "page": "🏗️ Govt Data Hub"},
            {"label": "GST & Legal",     "page": "🛡️ GST & Legal Monitor"},
            {"label": "Risk Scoring",    "page": "⚡ Risk Scoring"},
            {"label": "Change Log",      "page": "🔔 Change Notifications"},
        ],
    },
    "📊 Reports": {
        "icon": "📊", "label": "Reports",
        "tabs": [
            {"label": "Financial Intel",   "page": "💰 Financial Intelligence"},
            {"label": "Strategy & Export", "page": "📤 Reports",
             "also": ["🎯 Strategy Panel"]},
            {"label": "Past Predictions",  "page": "⏳ Past Predictions",
             "also": ["📁 PDF Archive"]},
            {"label": "Road Budget",       "page": "🛣️ Road Budget & Demand"},
        ],
    },
    "⚙ System Control": {
        "icon": "⚙", "label": "System Control",
        "tabs": [
            {"label": "Control Center",  "page": "🎛️ System Control Center",
             "also": ["🤖 AI Setup & Workers", "🏥 Health Monitor"]},
            {"label": "API Status",      "page": "🌐 API Dashboard",
             "also": ["🔗 API HUB"]},
            {"label": "Sync & Ops",      "page": "🔄 Sync Status",
             "also": ["🖥️ Ops Dashboard"]},
            {"label": "Settings",        "page": "⚙️ Settings",
             "also": ["📦 System Requirements"]},
        ],
    },
    "🛠 Developer": {
        "icon": "🛠", "label": "Developer",
        "tabs": [
            {"label": "Ops Map",         "page": "🛠️ Developer Ops Map"},
            {"label": "Flow Map",        "page": "🗺️ Dashboard Flow Map"},
            {"label": "Bug Tracker",     "page": "🐞 Bug Tracker",
             "also": ["🏥 System Health"]},
            {"label": "Dev Logs",        "page": "⚙️ Dev & System Activity",
             "also": ["🚨 Alert Center"]},
        ],
    },
    "🤖 AI & Knowledge": {
        "icon": "🤖", "label": "AI & Knowledge",
        "tabs": [
            {"label": "Trading Chatbot",   "page": "💬 Trading Chatbot",
             "also": ["🤖 AI Assistant", "🧠 AI Dashboard Assistant"]},
            {"label": "Fallback Engine",   "page": "🔄 AI Fallback Engine"},
            {"label": "Knowledge Base",    "page": "📚 Knowledge Base",
             "also": ["🏛️ Business Intelligence"]},
            {"label": "Intelligence Hub",  "page": "🎯 Intelligence Hub",
             "also": ["🤖 AI Learning", "📥 Contact Importer", "🛠️ Data Manager"]},
        ],
    },
}

# ── Sidebar rendering order (with divider markers) ───────────────────────────
SIDEBAR_ORDER: list[str] = [
    "🏠 Home",
    "📌 Director Briefing",
    "_divider_1",
    "💰 Procurement",
    "🧾 Sales",
    "🚚 Logistics",
    "🧠 Intelligence",
    "_divider_2",
    "🛡 Compliance",
    "📊 Reports",
    "_divider_3",
    "⚙ System Control",
    "🛠 Developer",
    "🤖 AI & Knowledge",
]


# ═══════════════════════════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════════

def get_tabs(module: str) -> list[dict]:
    """Return the sub-tab definitions for a module."""
    mod = MODULE_NAV.get(module)
    return mod["tabs"] if mod else []


def get_default_page(module: str) -> str:
    """Return the first tab's default page for a module."""
    tabs = get_tabs(module)
    return tabs[0]["page"] if tabs else "🏠 Home"


def get_module_for_page(page: str) -> str | None:
    """Reverse lookup: given an old page string, find which module owns it."""
    for mod_key, mod in MODULE_NAV.items():
        for tab in mod["tabs"]:
            if tab["page"] == page:
                return mod_key
            for also_pg in tab.get("also", []):
                if also_pg == page:
                    return mod_key
    return None


def get_subtab_idx_for_page(module: str, page: str) -> int:
    """Given a module and a page, return the tab index (0-3) that owns it."""
    for i, tab in enumerate(get_tabs(module)):
        if tab["page"] == page:
            return i
        if page in tab.get("also", []):
            return i
    return 0


def all_pages() -> list[str]:
    """Return a flat list of ALL page strings reachable through the nav."""
    pages: list[str] = []
    for mod in MODULE_NAV.values():
        for tab in mod["tabs"]:
            if not tab["page"].startswith("_"):
                pages.append(tab["page"])
            for p in tab.get("also", []):
                pages.append(p)
    return pages


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE-LEVEL ROLE REQUIREMENTS
# Minimum role needed to access each module.  Pages not listed default to
# "viewer".  Role hierarchy: director(4) > sales(3) > operations(2) > viewer(1)
# ═══════════════════════════════════════════════════════════════════════════════

MODULE_ROLE_MAP: dict[str, str] = {
    "🏠 Home":              "viewer",
    "📌 Director Briefing": "director",
    "💰 Procurement":       "operations",
    "🧾 Sales":             "sales",
    "🚚 Logistics":         "operations",
    "🧠 Intelligence":      "viewer",
    "🛡 Compliance":        "operations",
    "📊 Reports":           "viewer",
    "⚙ System Control":    "director",
    "🛠 Developer":         "director",
    "🤖 AI & Knowledge":   "viewer",
}


def _build_page_role_map() -> dict[str, str]:
    """Build a flat page → required role mapping from MODULE_ROLE_MAP."""
    mapping: dict[str, str] = {}
    for mod_key, mod in MODULE_NAV.items():
        required = MODULE_ROLE_MAP.get(mod_key, "viewer")
        for tab in mod["tabs"]:
            page = tab["page"]
            if not page.startswith("_"):
                mapping[page] = required
            for p in tab.get("also", []):
                mapping[p] = required
    return mapping


PAGE_ROLE_MAP: dict[str, str] = _build_page_role_map()
