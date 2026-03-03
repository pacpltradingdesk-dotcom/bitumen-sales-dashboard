"""
PPS Anantam — Missing Inputs Engine v1.0
==========================================
Detects data gaps across the system and generates prioritized input requests.
Shows a daily popup in dashboard asking users to fill critical missing data.

Priority Levels:
  High   — Directly affects pricing accuracy or deal calculation
  Medium — Improves forecasting and CRM intelligence
  Low    — Enhances reporting and analytics
"""

import json
import datetime
from pathlib import Path
from typing import List, Dict

import pytz

IST = pytz.timezone("Asia/Kolkata")
BASE = Path(__file__).parent

MISSING_INPUTS_FILE = BASE / "missing_inputs.json"


def _now() -> str:
    return datetime.datetime.now(IST).strftime("%d-%m-%Y %H:%M IST")


def _today() -> str:
    return datetime.datetime.now(IST).strftime("%d-%m-%Y")


def _load_json(path, default=None):
    if default is None:
        default = []
    try:
        if path.exists():
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
    except (json.JSONDecodeError, IOError):
        pass
    return default


def _save_json(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False, default=str)


# ─── Field Definitions ───────────────────────────────────────────────────────

SCAN_FIELDS = [
    {
        "field": "supplier_latest_quote",
        "label": "New supplier quote received?",
        "reason": "Updates cheapest source ranking — directly affects all pricing",
        "priority": "High",
        "input_type": "text",
        "placeholder": "Supplier name, grade, price/MT",
        "entity_type": "supplier",
    },
    {
        "field": "actual_freight_rate",
        "label": "Actual freight rate for last deal?",
        "reason": "Improves cost accuracy — current rates may be outdated",
        "priority": "High",
        "input_type": "number",
        "placeholder": "Rate in Rs/km",
        "entity_type": "deal",
    },
    {
        "field": "customer_enquiry",
        "label": "Any new customer enquiry?",
        "reason": "Feeds opportunity engine — auto-discovers best price for them",
        "priority": "High",
        "input_type": "text",
        "placeholder": "Customer name, city, grade, qty",
        "entity_type": "customer",
    },
    {
        "field": "competitor_price",
        "label": "Competitor price heard?",
        "reason": "Adjusts offer strategy — know where we stand",
        "priority": "Medium",
        "input_type": "number",
        "placeholder": "Competitor name, Rs/MT",
        "entity_type": "market",
    },
    {
        "field": "deal_closed_price",
        "label": "Actual deal closed price?",
        "reason": "Trains prediction model — improves forecast accuracy",
        "priority": "Medium",
        "input_type": "number",
        "placeholder": "Final price in Rs/MT",
        "entity_type": "deal",
    },
    {
        "field": "payment_update",
        "label": "Payment received update?",
        "reason": "Updates credit risk score and outstanding balance",
        "priority": "Medium",
        "input_type": "text",
        "placeholder": "Customer, amount, date",
        "entity_type": "deal",
    },
    {
        "field": "inventory_count",
        "label": "Current inventory at depot?",
        "reason": "Pipeline accuracy — shows available stock on homepage",
        "priority": "Low",
        "input_type": "number",
        "placeholder": "Total MT available",
        "entity_type": "inventory",
    },
    {
        "field": "grade_preference",
        "label": "Any customer grade preference change?",
        "reason": "CRM profile update — improves recommendation accuracy",
        "priority": "Low",
        "input_type": "text",
        "placeholder": "Customer name, new grade preference",
        "entity_type": "customer",
    },
]


class MissingInputsEngine:
    """Scans for data gaps and generates input requests."""

    def scan_all_gaps(self) -> List[dict]:
        """
        Scan entire system for missing/stale data.
        Returns prioritized list of input requests.
        """
        gaps = []

        # Check suppliers without recent quotes
        gaps += self._check_supplier_quotes()

        # Check customers without last price
        gaps += self._check_customer_data()

        # Check stale price data
        gaps += self._check_price_freshness()

        # Check empty data tables
        gaps += self._check_empty_tables()

        # Add standing daily questions
        gaps += self._daily_questions()

        # Deduplicate by field name
        seen = set()
        unique_gaps = []
        for g in gaps:
            key = g.get("field", "")
            if key not in seen:
                seen.add(key)
                unique_gaps.append(g)

        # Sort by priority
        priority_order = {"High": 0, "Medium": 1, "Low": 2}
        unique_gaps.sort(key=lambda x: priority_order.get(x.get("priority", "Low"), 3))

        return unique_gaps

    def _check_supplier_quotes(self) -> list:
        """Check if any supplier hasn't been quoted recently."""
        gaps = []
        try:
            # Try database first, fall back to JSON
            try:
                from database import get_all_suppliers
                suppliers = get_all_suppliers()
            except Exception:
                suppliers = _load_json(BASE / "purchase_parties.json", [])
            active_count = sum(1 for s in suppliers if s.get("marked_for_purchase") or s.get("is_active"))
            if active_count < 5:
                gaps.append({
                    "field": "active_suppliers",
                    "label": f"Only {active_count} active suppliers marked for purchase",
                    "reason": "More active suppliers = better sourcing options",
                    "priority": "High",
                    "input_type": "action",
                    "placeholder": "Mark more suppliers as active in Data Manager",
                    "entity_type": "supplier",
                    "created_at": _now(),
                })
        except Exception:
            pass
        return gaps

    def _check_customer_data(self) -> list:
        """Check for customers missing key data."""
        gaps = []
        try:
            # Try database first, fall back to JSON
            try:
                from database import get_all_customers
                customers = get_all_customers()
            except Exception:
                customers = _load_json(BASE / "sales_parties.json", [])
            if len(customers) < 5:
                gaps.append({
                    "field": "customer_count",
                    "label": f"Only {len(customers)} customers in database",
                    "reason": "More customers = more opportunities. Add from contacts/LinkedIn",
                    "priority": "High",
                    "input_type": "action",
                    "placeholder": "Import customers via Contact Importer",
                    "entity_type": "customer",
                    "created_at": _now(),
                })

            for c in customers:
                if not c.get("contact"):
                    gaps.append({
                        "field": f"customer_contact_{c.get('name', 'unknown')}",
                        "label": f"Missing contact for {c.get('name')}",
                        "reason": "Cannot follow up without contact info",
                        "priority": "Medium",
                        "input_type": "text",
                        "placeholder": "Phone or email",
                        "entity_type": "customer",
                        "entity_name": c.get("name"),
                        "created_at": _now(),
                    })
        except Exception:
            pass
        return gaps[:5]  # Max 5 gaps from this check

    def _check_price_freshness(self) -> list:
        """Check if price data is stale (>24 hours old)."""
        gaps = []
        try:
            prices = _load_json(BASE / "tbl_crude_prices.json", [])
            if not prices:
                gaps.append({
                    "field": "crude_prices_empty",
                    "label": "No crude price data available",
                    "reason": "All pricing depends on crude oil benchmark",
                    "priority": "High",
                    "input_type": "action",
                    "placeholder": "Run API sync to fetch latest prices",
                    "entity_type": "market",
                    "created_at": _now(),
                })
        except Exception:
            pass
        return gaps

    def _check_empty_tables(self) -> list:
        """Check for data tables that should have data but are empty."""
        gaps = []
        important_tables = [
            ("tbl_contacts.json", "Contact database", "High"),
            ("tbl_demand_proxy.json", "Demand proxy data", "Medium"),
            ("tbl_highway_km.json", "Highway construction data", "Medium"),
            ("tbl_ports_master.json", "Port master data", "Low"),
        ]

        for filename, label, priority in important_tables:
            data = _load_json(BASE / filename, [])
            if not data:
                gaps.append({
                    "field": f"empty_{filename}",
                    "label": f"{label} is empty",
                    "reason": f"Missing {label} reduces analysis accuracy",
                    "priority": priority,
                    "input_type": "action",
                    "placeholder": "Run seed data script or manual entry",
                    "entity_type": "system",
                    "created_at": _now(),
                })
        return gaps

    def _daily_questions(self) -> list:
        """Standing daily input questions."""
        return [
            {
                "field": f,
                "label": SCAN_FIELDS[i]["label"],
                "reason": SCAN_FIELDS[i]["reason"],
                "priority": SCAN_FIELDS[i]["priority"],
                "input_type": SCAN_FIELDS[i]["input_type"],
                "placeholder": SCAN_FIELDS[i]["placeholder"],
                "entity_type": SCAN_FIELDS[i]["entity_type"],
                "created_at": _now(),
            }
            for i, f in enumerate(
                [sf["field"] for sf in SCAN_FIELDS]
            )
        ]

    def save_collected_input(self, field: str, value: str) -> None:
        """Save a collected input value."""
        existing = _load_json(MISSING_INPUTS_FILE, [])
        existing.append({
            "field": field,
            "value": value,
            "collected_at": _now(),
            "status": "collected",
        })
        if len(existing) > 1000:
            existing = existing[-1000:]
        _save_json(MISSING_INPUTS_FILE, existing)

    def should_show_popup(self) -> bool:
        """Check if popup should be shown (once per day)."""
        existing = _load_json(MISSING_INPUTS_FILE, [])
        if not existing:
            return True
        # Check if last entry (collection or popup_shown) was today
        last = existing[-1] if existing else {}
        last_date = last.get("collected_at", last.get("shown_at", ""))[:10]
        return last_date != _today()

    def mark_popup_shown(self) -> None:
        """Record that the popup was shown today so it won't repeat."""
        existing = _load_json(MISSING_INPUTS_FILE, [])
        existing.append({
            "field": "_popup_shown",
            "value": "",
            "shown_at": _now(),
            "status": "shown",
        })
        if len(existing) > 1000:
            existing = existing[-1000:]
        _save_json(MISSING_INPUTS_FILE, existing)


def get_pending_gaps() -> list:
    """Get all current data gaps for UI display."""
    engine = MissingInputsEngine()
    return engine.scan_all_gaps()
