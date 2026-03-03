"""
PPS Anantam — Settings Engine v1.0
===================================
Configurable business rules for the entire dashboard ecosystem.
All pricing, tax, logistics, and sync settings in one place.
Stored in settings.json — editable via UI Settings page.
"""

import json
from pathlib import Path

BASE = Path(__file__).parent
SETTINGS_FILE = BASE / "settings.json"

# ─── Default Settings ─────────────────────────────────────────────────────────

DEFAULT_SETTINGS = {
    # Margin & Pricing
    "margin_min_per_mt": 500,
    "margin_balanced_multiplier": 1.6,
    "margin_premium_multiplier": 2.4,
    "gst_rate_pct": 18,
    "customs_duty_pct": 2.5,

    # Transport Rates
    "bulk_rate_per_km": 5.5,
    "drum_rate_per_km": 6.0,
    "decanter_conversion_cost": 500,

    # Import Cost Defaults
    "default_fob_usd": 380,
    "default_freight_usd": 35,
    "default_insurance_pct": 0.5,
    "default_switch_bl_usd": 2,
    "default_port_charges_inr": 10000,
    "default_cha_per_mt": 75,
    "default_handling_per_mt": 100,
    "default_vessel_qty_mt": 5000,

    # Quotation Defaults
    "quote_validity_hours": 24,
    "payment_default_terms": "100% Advance",

    # Alert Thresholds
    "price_alert_threshold_pct": 5,
    "crude_price_min_usd": 40,
    "crude_price_max_usd": 150,
    "bitumen_price_min_inr": 15000,
    "bitumen_price_max_inr": 120000,
    "fx_min_usdinr": 50,
    "fx_max_usdinr": 120,

    # Sync Schedule
    "sync_schedule_hour": 6,
    "sync_schedule_minute": 0,
    "sync_interval_minutes": 60,
    "news_fetch_interval_minutes": 10,

    # Currency
    "currency_default": "INR",
    "currency_international": "USD",

    # Ports
    "ports": [
        "Kandla", "Mundra", "Mangalore", "JNPT",
        "Karwar", "Haldia", "Ennore", "Paradip"
    ],

    # Grades
    "grades": [
        "VG30", "VG10", "VG40", "Emulsion",
        "CRMB-55", "CRMB-60", "PMB"
    ],

    # CRM Relationship Decay (days)
    "crm_hot_threshold_days": 7,
    "crm_warm_threshold_days": 30,
    "crm_cold_threshold_days": 90,

    # Data Retention
    "max_price_history_records": 5000,
    "max_news_articles": 5000,
    "max_sync_logs": 1000,
    "max_communication_records": 10000,

    # Email Engine
    "email_enabled": False,
    "email_rate_limit_per_hour": 50,
    "email_auto_send_offer": False,
    "email_auto_send_followup": False,
    "email_auto_send_reactivation": False,
    "email_auto_send_payment_reminder": False,
    "email_director_report_enabled": False,
    "email_director_report_time": "08:30",
    "email_director_report_to": "",
    "email_weekly_summary_enabled": False,
    "email_weekly_summary_day": "Monday",
    "email_weekly_summary_time": "09:00",
    "email_weekly_summary_to": "",
    "email_max_retries": 3,
    "email_retry_delay_minutes": 15,

    # WhatsApp Engine (360dialog)
    "whatsapp_enabled": False,
    "whatsapp_auto_send_offer": False,
    "whatsapp_auto_send_followup": False,
    "whatsapp_auto_send_reactivation": False,
    "whatsapp_auto_send_payment_reminder": False,
    "whatsapp_rate_limit_per_minute": 20,
    "whatsapp_rate_limit_per_day": 1000,
    "whatsapp_session_message_enabled": True,

    # AI Learning
    "ai_learning_enabled": True,
    "ai_learning_daily": True,
    "ai_learning_weekly": True,
    "ai_learning_monthly": True,

    # Infra Demand Intelligence
    "infra_demand_enabled": True,
    "gdelt_sync_interval_min": 120,
    "infra_backfill_months": 24,
}


def load_settings() -> dict:
    """Load settings from file, merge with defaults for any missing keys."""
    settings = dict(DEFAULT_SETTINGS)
    if SETTINGS_FILE.exists():
        try:
            with open(SETTINGS_FILE, "r", encoding="utf-8") as f:
                stored = json.load(f)
            if isinstance(stored, dict):
                settings.update(stored)
        except (json.JSONDecodeError, IOError):
            pass
    return settings


def save_settings(settings: dict) -> None:
    """Save settings to file."""
    with open(SETTINGS_FILE, "w", encoding="utf-8") as f:
        json.dump(settings, f, indent=2, ensure_ascii=False)


def get(key: str, default=None):
    """Get a single setting value."""
    settings = load_settings()
    return settings.get(key, default)


def update(key: str, value) -> None:
    """Update a single setting value."""
    settings = load_settings()
    settings[key] = value
    save_settings(settings)


def reset_to_defaults() -> None:
    """Reset all settings to defaults."""
    save_settings(DEFAULT_SETTINGS)


# Initialize settings file if it doesn't exist
if not SETTINGS_FILE.exists():
    save_settings(DEFAULT_SETTINGS)
