"""
PPS Anantam — Intelligent CRM Engine v2.0
==========================================
Market-intelligent CRM with auto-updating profiles,
relationship scoring, and AI-driven task management.

Features:
  - Task CRUD with pipeline automation
  - Customer/Supplier relationship intelligence
  - Auto-profile updates based on interaction frequency
  - Deal probability scoring
  - Today's target recommendations
"""

import json
import os
import datetime
import uuid
from datetime import timedelta
from pathlib import Path

import pytz

IST = pytz.timezone("Asia/Kolkata")
BASE = Path(__file__).parent

# --- CONFIGURATION ---
TASKS_FILE = "crm_tasks.json"
ACTIVITIES_FILE = "crm_activities.json"

# --- CONSTANTS ---
PIPELINE_STAGES = [
    "New Enquiry", "Quoted", "Negotiation", "PO Awaited",
    "Dispatch", "Delivered", "Payment Follow-up", "Closed Won", "Closed Lost"
]

TASK_TYPES = ["Call", "WhatsApp", "Email", "Meeting", "Internal"]
PRIORITIES = ["High", "Medium", "Low"]

# Relationship decay thresholds (days since last interaction)
RELATIONSHIP_THRESHOLDS = {"hot": 7, "warm": 30, "cold": 90}


# --- DATA HANDLING ---

def load_data(filepath, default=None):
    if default is None:
        default = []
    if os.path.exists(filepath):
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return default
    return default


def save_data(filepath, data):
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False, default=str)


def get_tasks(): return load_data(TASKS_FILE)
def save_tasks(tasks): save_data(TASKS_FILE, tasks)
def get_activities(): return load_data(ACTIVITIES_FILE)
def save_activities(acts): save_data(ACTIVITIES_FILE, acts)


# --- CORE CRM LOGIC ---

def add_task(client_name, task_type, due_date_str, priority="Medium", note="", automated=False):
    """
    Adds a new task to the CRM.
    due_date_str format: 'DD-MM-YYYY HH:MM'
    """
    tasks = get_tasks()
    new_task = {
        "id": str(uuid.uuid4())[:8],
        "client": client_name,
        "type": task_type,
        "due_date": due_date_str,
        "status": "Pending",
        "priority": priority,
        "note": note,
        "created_at": datetime.datetime.now(IST).strftime("%d-%m-%Y %H:%M"),
        "automated": automated
    }
    tasks.append(new_task)
    save_tasks(tasks)
    return new_task


def complete_task(task_id, outcome_note=""):
    tasks = get_tasks()
    matched = None
    for t in tasks:
        if t['id'] == task_id:
            t['status'] = "Completed"
            t['outcome'] = outcome_note
            t['completed_at'] = datetime.datetime.now(IST).strftime("%d-%m-%Y %H:%M")
            matched = t
            break
    save_tasks(tasks)
    if matched:
        log_activity(matched['client'], matched['type'],
                     f"Completed Task: {matched['note']} | Outcome: {outcome_note}")


def log_activity(client_name, act_type, details):
    acts = get_activities()
    new_act = {
        "id": str(uuid.uuid4())[:8],
        "client": client_name,
        "type": act_type,
        "details": details,
        "timestamp": datetime.datetime.now(IST).strftime("%d-%m-%Y %H:%M")
    }
    acts.append(new_act)
    # Keep max 5000 activities
    if len(acts) > 5000:
        acts = acts[-5000:]
    save_activities(acts)


# --- AUTOMATION ENGINE ( The "Brain" ) ---

def auto_generate_tasks(client_data, deal_stage):
    """
    Generates tasks based on rules.
    Extended with 6 automation rules.
    """
    now = datetime.datetime.now(IST)
    created_tasks = []

    # Rule 1: New Lead -> Call in 15 mins
    if deal_stage == "New Enquiry":
        due = (now + timedelta(minutes=15)).strftime("%d-%m-%Y %H:%M")
        add_task(client_data['name'], "Call", due, "High",
                 "New Enquiry: Introduction & Qualify", automated=True)
        created_tasks.append("New Lead Call")

    # Rule 2: Quote Sent -> Follow up in 2 hours + next day
    elif deal_stage == "Quoted":
        due = (now + timedelta(hours=2)).strftime("%d-%m-%Y %H:%M")
        add_task(client_data['name'], "WhatsApp", due, "High",
                 "Check if Quote Received", automated=True)
        due_next = (now + timedelta(days=1)).strftime("%d-%m-%Y %H:%M")
        add_task(client_data['name'], "Call", due_next, "Medium",
                 "Quote Follow-up / Negotiation", automated=True)
        created_tasks.append("Quote Follow-ups")

    # Rule 3: Payment Pending -> Daily Reminder
    elif deal_stage == "Payment Follow-up":
        due = (now + timedelta(days=1)).strftime("%d-%m-%Y %H:%M")
        add_task(client_data['name'], "Call", due, "High",
                 "Payment Reminder", automated=True)
        created_tasks.append("Payment Reminder")

    # Rule 4: Negotiation -> WhatsApp offer + Call in 1 day
    elif deal_stage == "Negotiation":
        due = (now + timedelta(hours=1)).strftime("%d-%m-%Y %H:%M")
        add_task(client_data['name'], "WhatsApp", due, "High",
                 "Send revised offer with 3-tier pricing", automated=True)
        due_call = (now + timedelta(days=1)).strftime("%d-%m-%Y %H:%M")
        add_task(client_data['name'], "Call", due_call, "High",
                 "Negotiation follow-up call", automated=True)
        created_tasks.append("Negotiation Tasks")

    # Rule 5: Delivered -> Thank you + Payment follow-up
    elif deal_stage == "Delivered":
        due = (now + timedelta(hours=2)).strftime("%d-%m-%Y %H:%M")
        add_task(client_data['name'], "WhatsApp", due, "Medium",
                 "Send delivery confirmation & thank you", automated=True)
        due_pay = (now + timedelta(days=3)).strftime("%d-%m-%Y %H:%M")
        add_task(client_data['name'], "Call", due_pay, "Medium",
                 "Payment schedule follow-up", automated=True)
        created_tasks.append("Post-Delivery Tasks")

    # Rule 6: Closed Lost -> Re-engage in 30 days
    elif deal_stage == "Closed Lost":
        due = (now + timedelta(days=30)).strftime("%d-%m-%Y %H:%M")
        add_task(client_data['name'], "Call", due, "Low",
                 "Re-engagement: Check if new opportunity exists", automated=True)
        created_tasks.append("Lost Deal Re-engagement")

    return created_tasks


def get_due_tasks(filter_type="Today"):
    """
    Filter options: Today, Overdue, Upcoming
    """
    all_tasks = get_tasks()
    pending = [t for t in all_tasks if t.get('status') == "Pending"]
    now_str = datetime.datetime.now(IST).strftime("%d-%m-%Y %H:%M")
    today_date = datetime.datetime.now(IST).strftime("%d-%m-%Y")

    filtered = []
    for t in pending:
        t_date = t.get('due_date', '').split(' ')[0]

        if filter_type == "Overdue":
            if t.get('due_date', '') < now_str:
                filtered.append(t)
        elif filter_type == "Today":
            if t_date == today_date and t.get('due_date', '') >= now_str:
                filtered.append(t)
        elif filter_type == "Upcoming":
            if t_date > today_date:
                filtered.append(t)

    return sorted(filtered, key=lambda x: x.get('due_date', ''))


# ─── INTELLIGENT CRM ────────────────────────────────────────────────────────

class IntelligentCRM:
    """Market-intelligent CRM with auto-updating profiles."""

    def __init__(self):
        try:
            from settings_engine import load_settings
            self.settings = load_settings()
        except Exception:
            self.settings = {}
        self.hot_days = self.settings.get("crm_hot_threshold_days", 7)
        self.warm_days = self.settings.get("crm_warm_threshold_days", 30)
        self.cold_days = self.settings.get("crm_cold_threshold_days", 90)

    def get_customer_profile(self, customer_name: str) -> dict:
        """
        Complete customer profile with intelligence.

        Returns: {
            basic, purchase_history, intelligence, recommendations
        }
        """
        profile = {
            "basic": {"name": customer_name},
            "purchase_history": [],
            "intelligence": {},
            "recommendations": {},
        }

        # Try database first
        try:
            from database import get_all_customers
            customers = get_all_customers()
            for c in customers:
                if c.get("name", "").lower() == customer_name.lower():
                    profile["basic"] = {
                        "name": c.get("name"),
                        "city": c.get("city"),
                        "state": c.get("state"),
                        "contact": c.get("contact"),
                        "gstin": c.get("gstin"),
                        "category": c.get("category"),
                    }
                    profile["intelligence"] = {
                        "last_price": c.get("last_purchase_price"),
                        "last_date": c.get("last_purchase_date"),
                        "last_qty": c.get("last_purchase_qty"),
                        "preferred_grade": c.get("preferred_grades"),
                        "credit_terms": c.get("credit_terms"),
                        "expected_monthly_demand": c.get("expected_monthly_demand"),
                        "outstanding": c.get("outstanding_inr", 0),
                        "relationship_stage": c.get("relationship_stage", "cold"),
                        "next_followup": c.get("next_followup_date"),
                    }
                    break
        except Exception:
            pass

        # Fallback to sales_parties.json
        if not profile["basic"].get("city"):
            try:
                parties = json.loads((BASE / "sales_parties.json").read_text(encoding="utf-8"))
                for p in parties:
                    if p.get("name", "").lower() == customer_name.lower():
                        profile["basic"].update({
                            "city": p.get("city"),
                            "state": p.get("state"),
                            "category": p.get("category"),
                        })
                        break
            except Exception:
                pass

        # Activity history
        activities = get_activities()
        customer_acts = [a for a in activities if a.get("client", "").lower() == customer_name.lower()]
        profile["purchase_history"] = customer_acts[-20:]

        # Days since last interaction
        if customer_acts:
            try:
                last_ts = customer_acts[-1].get("timestamp", "")
                last_dt = datetime.datetime.strptime(last_ts, "%d-%m-%Y %H:%M")
                days_since = (datetime.datetime.now() - last_dt).days
                profile["intelligence"]["days_since_last_contact"] = days_since
                profile["intelligence"]["auto_relationship_stage"] = (
                    "hot" if days_since <= self.hot_days else
                    "warm" if days_since <= self.warm_days else
                    "cold"
                )
            except Exception:
                pass

        # Recommendations
        city = profile["basic"].get("city", "")
        if city:
            try:
                from calculation_engine import BitumenCalculationEngine
                calc = BitumenCalculationEngine()
                sources = calc.find_best_sources(city, grade="VG30", top_n=1)
                if sources:
                    best = sources[0]
                    landed = best.get("landed_cost", 0)
                    profile["recommendations"] = {
                        "best_price_today": landed + 500,
                        "best_source": best.get("source", ""),
                        "margin_at_best_price": 500,
                        "suggested_action": self._suggest_action(profile),
                    }
            except Exception:
                pass

        return profile

    def _suggest_action(self, profile: dict) -> str:
        """Generate suggested action based on profile data."""
        stage = profile.get("intelligence", {}).get("auto_relationship_stage", "cold")
        days = profile.get("intelligence", {}).get("days_since_last_contact", 999)

        if stage == "hot":
            return "Active customer. Check for repeat order opportunity."
        elif stage == "warm":
            return f"Last contact {days} days ago. Send updated price offer."
        else:
            return f"Cold customer ({days} days). Reactivation call recommended."

    def auto_update_all_profiles(self):
        """
        Called daily by sync engine to update all customer profiles.
        Updates relationship_stage based on interaction frequency.
        """
        updated = 0
        try:
            from database import get_all_customers, _get_conn
            customers = get_all_customers()
            activities = get_activities()

            for cust in customers:
                name = cust.get("name", "")
                if not name:
                    continue

                # Find last activity for this customer
                cust_acts = [a for a in activities if a.get("client", "").lower() == name.lower()]
                if cust_acts:
                    try:
                        last_ts = cust_acts[-1].get("timestamp", "")
                        last_dt = datetime.datetime.strptime(last_ts, "%d-%m-%Y %H:%M")
                        days_since = (datetime.datetime.now() - last_dt).days
                        new_stage = (
                            "hot" if days_since <= self.hot_days else
                            "warm" if days_since <= self.warm_days else
                            "cold"
                        )

                        # Update in database
                        conn = _get_conn()
                        conn.execute(
                            "UPDATE customers SET relationship_stage = ?, updated_at = ? WHERE name = ?",
                            (new_stage, datetime.datetime.now(IST).strftime("%d-%m-%Y %H:%M IST"), name)
                        )
                        conn.commit()
                        conn.close()
                        updated += 1
                    except Exception:
                        pass
        except Exception:
            pass

        return updated

    def get_crm_summary(self) -> dict:
        """Get CRM dashboard summary stats."""
        tasks = get_tasks()
        activities = get_activities()

        pending = [t for t in tasks if t.get("status") == "Pending"]
        completed = [t for t in tasks if t.get("status") == "Completed"]
        high_pri = [t for t in pending if t.get("priority") == "High"]

        # Count by type
        type_counts = {}
        for t in pending:
            tp = t.get("type", "Other")
            type_counts[tp] = type_counts.get(tp, 0) + 1

        # Customer stats
        try:
            from database import get_dashboard_stats
            db_stats = get_dashboard_stats()
        except Exception:
            db_stats = {}

        return {
            "total_tasks": len(tasks),
            "pending": len(pending),
            "completed": len(completed),
            "high_priority": len(high_pri),
            "tasks_by_type": type_counts,
            "total_activities": len(activities),
            "total_customers": db_stats.get("total_customers", 0),
            "total_suppliers": db_stats.get("total_suppliers", 0),
            "active_deals": db_stats.get("active_deals", 0),
        }

    def get_todays_targets(self) -> dict:
        """
        AI-selected target lists for today.

        Returns: {
            calls_due, whatsapp_due, emails_due,
            overdue_tasks, hot_customers, cold_reactivations
        }
        """
        today_tasks = get_due_tasks("Today")
        overdue = get_due_tasks("Overdue")

        calls = [t for t in today_tasks if t.get("type") == "Call"]
        whatsapp = [t for t in today_tasks if t.get("type") == "WhatsApp"]
        emails = [t for t in today_tasks if t.get("type") == "Email"]

        # Hot customers (from database)
        hot_customers = []
        cold_reactivations = []
        try:
            from database import get_all_customers
            customers = get_all_customers()
            for c in customers:
                stage = c.get("relationship_stage", "cold")
                if stage == "hot":
                    hot_customers.append(c.get("name", ""))
                elif stage == "cold" and c.get("last_purchase_price"):
                    cold_reactivations.append({
                        "name": c.get("name"),
                        "city": c.get("city"),
                        "last_price": c.get("last_purchase_price"),
                    })
        except Exception:
            pass

        return {
            "calls_due": calls,
            "whatsapp_due": whatsapp,
            "emails_due": emails,
            "overdue_tasks": overdue,
            "hot_customers": hot_customers,
            "cold_reactivations": cold_reactivations[:10],
        }


# --- MOCK DATA GENERATOR ---
def init_mock_crm_data():
    if not os.path.exists(TASKS_FILE):
        now = datetime.datetime.now(IST)
        mock_tasks = [
            {"id": "101", "client": "L&T Construction", "type": "Call",
             "due_date": now.strftime("%d-%m-%Y 10:00"), "status": "Pending",
             "priority": "High", "note": "Negotiate Pricing for 50MT"},
            {"id": "102", "client": "Patel Infra", "type": "WhatsApp",
             "due_date": now.strftime("%d-%m-%Y 14:00"), "status": "Pending",
             "priority": "Medium", "note": "Send revised quote"},
            {"id": "103", "client": "Global Roadways", "type": "Email",
             "due_date": (now - timedelta(days=1)).strftime("%d-%m-%Y 09:00"),
             "status": "Pending", "priority": "Low",
             "note": "Intro email (Overdue)"}
        ]
        save_tasks(mock_tasks)


init_mock_crm_data()
