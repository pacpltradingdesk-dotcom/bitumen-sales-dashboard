
import json
import os
import datetime
import uuid
from datetime import timedelta

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

# --- DATA HANDLING ---

def load_data(filepath, default=[]):
    if os.path.exists(filepath):
        try:
            with open(filepath, 'r') as f:
                return json.load(f)
        except:
            return default
    return default

def save_data(filepath, data):
    with open(filepath, 'w') as f:
        json.dump(data, f, indent=2)

def get_tasks(): return load_data(TASKS_FILE)
def save_tasks(tasks): save_data(TASKS_FILE, tasks)
def get_activities(): return load_data(ACTIVITIES_FILE)
def save_activities(acts): save_data(ACTIVITIES_FILE, acts)

# --- CORE CRM LOGIC ---

def add_task(client_name, task_type, due_date_str, priority="Medium", note="", automated=False):
    """
    Adds a new task to the CRM.
    due_date_str format: 'YYYY-MM-DD HH:MM'
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
        "created_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M"),
        "automated": automated
    }
    tasks.append(new_task)
    save_tasks(tasks)
    return new_task

def complete_task(task_id, outcome_note=""):
    tasks = get_tasks()
    for t in tasks:
        if t['id'] == task_id:
            t['status'] = "Completed"
            t['outcome'] = outcome_note
            t['completed_at'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
            break
    save_tasks(tasks)
    # Log as activity automatically
    if t:
        log_activity(t['client'], t['type'], f"Completed Task: {t['note']} | Outcome: {outcome_note}")

def log_activity(client_name, act_type, details):
    acts = get_activities()
    new_act = {
        "id": str(uuid.uuid4())[:8],
        "client": client_name,
        "type": act_type,
        "details": details,
        "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    }
    acts.append(new_act)
    save_activities(acts)

# --- AUTOMATION ENGINE ( The "Brain" ) ---

def auto_generate_tasks(client_data, deal_stage):
    """
    Generates tasks based on rules.
    """
    now = datetime.datetime.now()
    created_tasks = []

    # Rule 1: New Lead -> Call in 15 mins
    if deal_stage == "New Enquiry":
        due = (now + timedelta(minutes=15)).strftime("%Y-%m-%d %H:%M")
        add_task(client_data['name'], "Call", due, "High", "🚀 New Enquiry: Introduction & Qualify", automated=True)
        created_tasks.append("New Lead Call")

    # Rule 2: Quote Sent -> Follow up in 2 hours
    elif deal_stage == "Quoted":
        due = (now + timedelta(hours=2)).strftime("%Y-%m-%d %H:%M")
        add_task(client_data['name'], "WhatsApp", due, "High", "💬 Check if Quote Received", automated=True)
        
        due_next = (now + timedelta(days=1)).strftime("%Y-%m-%d %H:%M")
        add_task(client_data['name'], "Call", due_next, "Medium", "📞 Quote Follow-up / Negotiation", automated=True)
        created_tasks.append("Quote Follow-ups")

    # Rule 3: Payment Pending -> Daily Reminder
    elif deal_stage == "Payment Follow-up":
        due = (now + timedelta(days=1)).strftime("%Y-%m-%d %H:%M")
        add_task(client_data['name'], "Call", due, "High", "💰 Payment Reminder", automated=True) 
        created_tasks.append("Payment Reminder")

    return created_tasks

def get_due_tasks(filter_type="Today"):
    """
    Filter options: Today, Overdue, Upcoming
    """
    all_tasks = get_tasks()
    pending = [t for t in all_tasks if t['status'] == "Pending"]
    now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    today_date = datetime.datetime.now().strftime("%Y-%m-%d")
    
    filtered = []
    for t in pending:
        t_date = t['due_date'].split(' ')[0]
        
        if filter_type == "Overdue":
            if t['due_date'] < now_str: # Simple string comparison works for ISO format
                filtered.append(t)
        elif filter_type == "Today":
            if t_date == today_date and t['due_date'] >= now_str:
                filtered.append(t)
        elif filter_type == "Upcoming":
            if t_date > today_date:
                filtered.append(t)
                
    # Sort by date
    return sorted(filtered, key=lambda x: x['due_date'])

# --- MOCK DATA GENERATOR ---
def init_mock_crm_data():
    if not os.path.exists(TASKS_FILE):
        mock_tasks = [
            {"id": "101", "client": "L&T Construction", "type": "Call", "due_date": datetime.datetime.now().strftime("%Y-%m-%d 10:00"), "status": "Pending", "priority": "High", "note": "Negotiate Pricing for 50MT"},
            {"id": "102", "client": "Patel Infra", "type": "WhatsApp", "due_date": datetime.datetime.now().strftime("%Y-%m-%d 14:00"), "status": "Pending", "priority": "Medium", "note": "Send revised quote"},
            {"id": "103", "client": "Global Roadways", "type": "Email", "due_date": (datetime.datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d 09:00"), "status": "Pending", "priority": "Low", "note": "Intro email (Overdue)"}
        ]
        save_tasks(mock_tasks)

init_mock_crm_data()
