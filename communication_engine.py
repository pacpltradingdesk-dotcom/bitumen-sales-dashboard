"""
PPS Anantam — Communication Hub Engine v1.0
=============================================
Auto-generates WhatsApp, Email, and Call scripts for sales team.
Templates are market-intelligent — prices, savings, and context auto-populated.

Communication Types:
  - offer: New pricing offer
  - followup: Deal follow-up
  - reactivation: Re-engage cold customer
  - tender_response: Tender/bid communication
  - payment_reminder: Payment follow-up
"""

import json
import datetime
from pathlib import Path
from typing import Dict, List, Optional

import pytz

IST = pytz.timezone("Asia/Kolkata")
BASE = Path(__file__).parent

COMM_LOG_FILE = BASE / "communication_log.json"


def _now() -> str:
    return datetime.datetime.now(IST).strftime("%d-%m-%Y %H:%M IST")


def _today() -> str:
    return datetime.datetime.now(IST).strftime("%d-%m-%Y")


def _fmt_inr(amount) -> str:
    """Format INR with Indian comma system."""
    if amount is None:
        return "N/A"
    try:
        amount = float(amount)
        if amount < 0:
            return f"-{_fmt_inr(-amount)}"
        s = f"{amount:,.0f}"
        integer_part = s.replace(",", "")
        if len(integer_part) <= 3:
            return f"\u20b9{integer_part}"
        last3 = integer_part[-3:]
        remaining = integer_part[:-3]
        groups = []
        while remaining:
            groups.insert(0, remaining[-2:])
            remaining = remaining[:-2]
        return f"\u20b9{','.join(groups)},{last3}"
    except (ValueError, TypeError):
        return str(amount)


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


# ─────────────────────────────────────────────────────────────────────────────
# COMMUNICATION HUB
# ─────────────────────────────────────────────────────────────────────────────

class CommunicationHub:
    """WhatsApp, Email, and Call script generator for bitumen sales."""

    def __init__(self):
        from settings_engine import load_settings
        self.settings = load_settings()
        self.company = "PPS Anantam"
        self.validity_hours = self.settings.get("quote_validity_hours", 24)
        self.payment_terms = self.settings.get("payment_default_terms", "100% Advance")

    # ─── WhatsApp Templates ──────────────────────────────────────────────────

    def whatsapp_offer(self, customer_name: str, city: str, grade: str,
                       quantity_mt: float, price_per_mt: float,
                       source: str = "", benefit_pct: float = 0) -> str:
        """Generate WhatsApp offer message."""
        benefit_line = ""
        if benefit_pct > 0:
            benefit_line = f"\nYour Benefit: *{benefit_pct}% cheaper* than market\n"

        return (
            f"*BITUMEN OFFER \u2014 {_today()}*\n"
            f"\ud83c\udfe2 {customer_name} | {city}\n"
            f"\n"
            f"\ud83c\udfd7\ufe0f Grade: *{grade}*\n"
            f"\ud83d\udce6 Qty: *{quantity_mt:.0f} MT*\n"
            f"\ud83d\udcb0 Rate: *{_fmt_inr(price_per_mt)}/MT* (Landed {city})\n"
            f"{benefit_line}"
            f"\n"
            f"\ud83d\ude9a Dispatch: Within 48 hours\n"
            f"\u23f3 Validity: {self.validity_hours} hours only\n"
            f"\ud83d\udcb3 Payment: {self.payment_terms}\n"
            f"\n"
            f"Reply *CONFIRM* to lock this price\n"
            f"\n"
            f"\u2014 {self.company}"
        )

    def whatsapp_followup(self, customer_name: str, reference: str = "",
                          days_since: int = 1) -> str:
        """Generate WhatsApp follow-up message."""
        return (
            f"Dear {customer_name},\n\n"
            f"Following up on our bitumen offer"
            f"{f' ({reference})' if reference else ''}.\n\n"
            f"The rate shared is still valid. Would you like to proceed?\n\n"
            f"We can dispatch within 48 hours of confirmation.\n\n"
            f"Please let us know.\n"
            f"\u2014 {self.company}"
        )

    def whatsapp_reactivation(self, customer_name: str, city: str,
                              new_price: float, old_price: float,
                              savings: float) -> str:
        """Generate WhatsApp message for reactivating cold customer."""
        return (
            f"Dear {customer_name},\n\n"
            f"Market prices have come down significantly.\n\n"
            f"We can now offer *VG-30 at {_fmt_inr(new_price)}/MT* landed {city}.\n"
            f"Your last rate was {_fmt_inr(old_price)}/MT.\n"
            f"\ud83d\udcb0 *You save {_fmt_inr(savings)}/MT*\n\n"
            f"This is a limited-time opportunity.\n"
            f"Reply *YES* for a formal quote.\n\n"
            f"\u2014 {self.company}"
        )

    def whatsapp_payment_reminder(self, customer_name: str, amount: float,
                                  invoice_ref: str = "", days_overdue: int = 0) -> str:
        """Generate WhatsApp payment reminder."""
        urgency = "Gentle reminder" if days_overdue < 7 else "Urgent request"
        return (
            f"Dear {customer_name},\n\n"
            f"{urgency}: Payment of {_fmt_inr(amount)} is pending"
            f"{f' ({days_overdue} days overdue)' if days_overdue > 0 else ''}.\n"
            f"{f'Ref: {invoice_ref}' if invoice_ref else ''}\n\n"
            f"Please arrange at the earliest.\n\n"
            f"Bank: ICICI Bank\n"
            f"A/C: 184105001402\n"
            f"IFSC: ICIC0001841\n\n"
            f"Thank you.\n"
            f"\u2014 {self.company}"
        )

    # ─── Email Templates ─────────────────────────────────────────────────────

    def email_offer(self, customer_name: str, city: str, grade: str,
                    quantity_mt: float, price_per_mt: float,
                    source: str = "", benefit_pct: float = 0) -> Dict[str, str]:
        """Generate email offer with subject + body."""
        subject = f"Bitumen {grade} Offer \u2014 {_fmt_inr(price_per_mt)}/MT Landed {city} | {self.company}"

        body = (
            f"Dear {customer_name},\n\n"
            f"We are pleased to offer the following bitumen supply for your {city} operations:\n\n"
            f"OFFER DETAILS:\n"
            f"  Product:     Bitumen {grade}\n"
            f"  Quantity:    {quantity_mt:.0f} MT\n"
            f"  Rate:        {_fmt_inr(price_per_mt)} per MT (Landed {city})\n"
            f"  Source:      {source if source else 'Best available'}\n"
            f"  HSN Code:    27132000\n"
            f"  GST:         18% (included in above rate)\n\n"
        )

        if benefit_pct > 0:
            body += f"  YOUR BENEFIT: {benefit_pct}% below current market rates\n\n"

        body += (
            f"TERMS & CONDITIONS:\n"
            f"  1. Payment:    {self.payment_terms}\n"
            f"  2. Dispatch:   Within 48 hours of payment realization\n"
            f"  3. Validity:   {self.validity_hours} hours from this email\n"
            f"  4. Delivery:   Ex-Terminal basis; freight included in above rate\n"
            f"  5. Transit:    Risk passes to buyer after loading\n"
            f"  6. Disputes:   Subject to Vadodara jurisdiction\n\n"
            f"Please confirm at your earliest to lock this rate.\n\n"
            f"Best regards,\n"
            f"PPS Anantam Corporation Pvt. Ltd.\n"
            f"Vadodara, Gujarat\n"
            f"GST: 24AAHCV1611L2ZD"
        )

        return {"subject": subject, "body": body}

    def email_followup(self, customer_name: str, original_date: str = "",
                       price: float = 0, city: str = "") -> Dict[str, str]:
        """Generate email follow-up."""
        subject = f"Follow-up: Bitumen Offer for {city} | {self.company}"
        body = (
            f"Dear {customer_name},\n\n"
            f"This is a follow-up to our bitumen offer"
            f"{f' dated {original_date}' if original_date else ''}.\n\n"
        )
        if price > 0:
            body += f"The offered rate of {_fmt_inr(price)}/MT is still available.\n\n"
        body += (
            f"We would appreciate your confirmation or any feedback.\n\n"
            f"Looking forward to your response.\n\n"
            f"Best regards,\n"
            f"{self.company}"
        )
        return {"subject": subject, "body": body}

    def email_reactivation(self, customer_name: str, city: str,
                           new_price: float, old_price: float,
                           savings: float) -> Dict[str, str]:
        """Generate reactivation email with price comparison."""
        subject = f"Prices Down! Bitumen VG30 at {_fmt_inr(new_price)}/MT for {city} | {self.company}"
        body = (
            f"Dear {customer_name},\n\n"
            f"We hope this finds you well. We noticed it's been a while since our last transaction "
            f"and wanted to share some excellent news.\n\n"
            f"Market prices have come down significantly:\n\n"
            f"  Your last rate:   {_fmt_inr(old_price)}/MT\n"
            f"  New offer rate:   {_fmt_inr(new_price)}/MT (Landed {city})\n"
            f"  YOUR SAVINGS:     {_fmt_inr(savings)}/MT\n\n"
            f"This is a limited-time opportunity as crude prices remain volatile.\n\n"
            f"We can dispatch within 48 hours of confirmation. Minimum order: 20 MT.\n\n"
            f"Looking forward to resuming our association.\n\n"
            f"Best regards,\n"
            f"PPS Anantam Corporation Pvt. Ltd.\n"
            f"Vadodara, Gujarat\n"
            f"GST: 24AAHCV1611L2ZD"
        )
        return {"subject": subject, "body": body}

    def email_payment_reminder(self, customer_name: str, amount: float,
                               invoice_ref: str = "", days_overdue: int = 0) -> Dict[str, str]:
        """Generate payment reminder email."""
        urgency = "Gentle Reminder" if days_overdue < 7 else "Urgent: Payment Overdue"
        subject = f"{urgency} — {_fmt_inr(amount)} Pending | {self.company}"
        body = (
            f"Dear {customer_name},\n\n"
            f"This is a {'gentle reminder' if days_overdue < 7 else 'follow-up'} regarding "
            f"the outstanding payment of {_fmt_inr(amount)}"
            f"{f' ({days_overdue} days overdue)' if days_overdue > 0 else ''}.\n"
        )
        if invoice_ref:
            body += f"\nInvoice Reference: {invoice_ref}\n"
        body += (
            f"\nKindly arrange the payment at your earliest convenience.\n\n"
            f"BANK DETAILS:\n"
            f"  Bank:    ICICI Bank\n"
            f"  A/C No:  184105001402\n"
            f"  IFSC:    ICIC0001841\n"
            f"  Name:    PPS Anantam Corporation Pvt. Ltd.\n\n"
            f"If the payment has already been made, please disregard this email "
            f"and share the transaction reference for our records.\n\n"
            f"Thank you for your prompt attention.\n\n"
            f"Best regards,\n"
            f"{self.company}"
        )
        return {"subject": subject, "body": body}

    def email_tender_response(self, authority_name: str, tender_ref: str,
                              grade: str, quantity_mt: float,
                              price_per_mt: float, delivery_location: str) -> Dict[str, str]:
        """Generate formal tender/bid response email."""
        subject = f"Bid Submission — {tender_ref} | Bitumen {grade} | {self.company}"
        body = (
            f"To,\n"
            f"The Procurement Officer,\n"
            f"{authority_name}\n\n"
            f"Subject: Bid Submission for {tender_ref}\n\n"
            f"Dear Sir/Madam,\n\n"
            f"With reference to your tender {tender_ref}, we are pleased to submit "
            f"our bid for the supply of bitumen as follows:\n\n"
            f"BID DETAILS:\n"
            f"  Product:        Bitumen {grade}\n"
            f"  Quantity:        {quantity_mt:.0f} MT\n"
            f"  Unit Rate:       {_fmt_inr(price_per_mt)} per MT\n"
            f"  Total Value:     {_fmt_inr(price_per_mt * quantity_mt)}\n"
            f"  HSN Code:        27132000\n"
            f"  GST:             18%\n"
            f"  Delivery:        {delivery_location}\n"
            f"  Dispatch:        Within 7 working days of PO\n\n"
            f"COMPANY DETAILS:\n"
            f"  Name:    PPS Anantam Corporation Pvt. Ltd.\n"
            f"  Address: Vadodara, Gujarat\n"
            f"  GST:     24AAHCV1611L2ZD\n\n"
            f"We confirm full compliance with the tender terms and conditions.\n\n"
            f"Thanking you,\n"
            f"For PPS Anantam Corporation Pvt. Ltd."
        )
        return {"subject": subject, "body": body}

    def email_director_report_template(self, briefing: dict) -> Dict[str, str]:
        """Generate HTML-formatted director daily report email."""
        yesterday = briefing.get("yesterday_summary", {})
        today_actions = briefing.get("today_actions", {})
        outlook = briefing.get("outlook", {})

        subject = f"Daily Intelligence Report — {briefing.get('date', _today())} | {self.company}"

        body = (
            f"DAILY INTELLIGENCE REPORT — {briefing.get('date', _today())}\n"
            f"{'=' * 50}\n\n"
            f"YESTERDAY SUMMARY:\n"
            f"  Deals Closed:        {yesterday.get('deals_closed', 0)}\n"
            f"  New Enquiries:       {yesterday.get('new_enquiries', 0)}\n"
            f"  Comms Sent:          {yesterday.get('communications_sent', 0)}\n"
            f"  Payments Received:   {_fmt_inr(yesterday.get('payments_received', 0))}\n"
            f"  Outstanding:         {_fmt_inr(yesterday.get('total_outstanding', 0))}\n\n"
            f"TODAY'S ACTIONS:\n"
        )
        for buyer in today_actions.get("buyers_to_call", [])[:5]:
            body += f"  - Call: {buyer.get('name', 'N/A')} ({buyer.get('city', '')})\n"
        for fu in today_actions.get("followups_due", [])[:5]:
            body += f"  - Follow-up: {fu.get('customer_name', fu.get('name', 'N/A'))}\n"

        if outlook:
            body += (
                f"\n15-DAY OUTLOOK:\n"
                f"  Demand Score:    {outlook.get('demand_score', {}).get('total', 'N/A')}/100\n"
                f"  Price Direction: {outlook.get('price_direction', {}).get('direction', 'N/A')}\n"
                f"  Strategy:        {outlook.get('stock_strategy', {}).get('action', 'N/A')}\n"
            )

        body += (
            f"\n{'=' * 50}\n"
            f"Generated by PPS Anantam Agentic AI Eco System\n"
            f"Vadodara, Gujarat | GST: 24AAHCV1611L2ZD"
        )
        return {"subject": subject, "body": body}

    def email_weekly_summary_template(self, summary: dict) -> Dict[str, str]:
        """Generate weekly summary email."""
        subject = f"Weekly Business Summary — {summary.get('week', _today())} | {self.company}"
        body = (
            f"WEEKLY BUSINESS SUMMARY\n"
            f"{'=' * 50}\n"
            f"Week: {summary.get('week', _today())}\n\n"
            f"DEAL PERFORMANCE:\n"
            f"  Total Deals:       {summary.get('total_deals', 0)}\n"
            f"  Revenue:           {_fmt_inr(summary.get('total_revenue', 0))}\n"
            f"  Avg Margin/MT:     {_fmt_inr(summary.get('avg_margin_per_mt', 0))}\n\n"
            f"MARKET MOVEMENT:\n"
            f"  Brent (Start):     ${summary.get('brent_start', 0):.2f}\n"
            f"  Brent (End):       ${summary.get('brent_end', 0):.2f}\n"
            f"  USD/INR:           {summary.get('fx_rate', 0):.2f}\n\n"
            f"CRM ACTIVITY:\n"
            f"  Communications:    {summary.get('total_comms', 0)}\n"
            f"  New Customers:     {summary.get('new_customers', 0)}\n"
            f"  Followups Done:    {summary.get('followups_done', 0)}\n\n"
            f"{'=' * 50}\n"
            f"Generated by PPS Anantam Agentic AI Eco System"
        )
        return {"subject": subject, "body": body}

    # ─── Call Scripts ────────────────────────────────────────────────────────

    def call_script_offer(self, customer_name: str, city: str, grade: str,
                          price: float, source: str = "",
                          savings: float = 0) -> str:
        """Generate structured call script for new offer."""
        objection_handling = (
            '   Price too high:\n'
            '     "This is our best landed cost including freight and GST. '
            'Compare with your current supplier\'s all-inclusive rate."\n\n'
            '   Need time to decide:\n'
            '     "I understand. However, this rate is valid for 24 hours only '
            'as crude prices are volatile. Shall I hold it for you?"\n\n'
            '   Competitor is cheaper:\n'
            '     "Could you share their rate? We can match or explain '
            'the quality/reliability difference."\n\n'
            '   Payment terms:\n'
            '     "Our standard is advance payment. For trusted partners, '
            'we can discuss 50-50 or PDC terms after 2-3 successful orders."'
        )

        result = (
            f"=== CALL SCRIPT: {customer_name} ({city}) ===\n\n"
            f"1. OPENING (30 sec):\n"
            f'   "Hello {customer_name}, this is [Your Name] from PPS Anantam, Vadodara. '
            f'How are you?"\n\n'
            f"2. MARKET CONTEXT (1 min):\n"
            f'   "I am calling because crude prices have moved favorably and we have '
            f'a very competitive {grade} rate available for {city} delivery."\n\n'
            f"3. OUR OFFER (1 min):\n"
            f'   "We can deliver {grade} at {_fmt_inr(price)} per MT landed {city}. '
            f'Source: {source if source else "our terminal"}."\n'
        )
        if savings > 0:
            result += (
                f'   "This is {_fmt_inr(savings)} per MT below current market rates."\n'
            )
        result += (
            f"\n4. HANDLE OBJECTIONS:\n{objection_handling}\n\n"
            f"5. CLOSE (30 sec):\n"
            f'   "Shall I send you a formal quotation on WhatsApp right now? '
            f'I can also prepare a detailed cost breakdown if needed."\n\n'
            f"6. NEXT STEPS:\n"
            f"   - If YES: Send WhatsApp quote immediately\n"
            f"   - If MAYBE: Schedule callback in 2 hours\n"
            f"   - If NO: Note reason, follow up in 1 week\n"
            f"=== END SCRIPT ==="
        )
        return result

    # ─── Follow-up Sequence Generator ────────────────────────────────────────

    def generate_followup_sequence(self, customer_name: str, city: str,
                                   price: float, grade: str = "VG30") -> List[dict]:
        """
        Generate 5-touch follow-up plan.

        Returns list of {day, channel, action, template_type}
        """
        return [
            {
                "day": 0,
                "channel": "WhatsApp",
                "action": f"Send initial offer to {customer_name}",
                "template_type": "offer",
                "status": "pending"
            },
            {
                "day": 1,
                "channel": "Call",
                "action": f"Follow-up call to {customer_name}",
                "template_type": "call_followup",
                "status": "pending"
            },
            {
                "day": 3,
                "channel": "Email",
                "action": f"Email with market update to {customer_name}",
                "template_type": "email_followup",
                "status": "pending"
            },
            {
                "day": 7,
                "channel": "WhatsApp",
                "action": f"WhatsApp check-in with {customer_name}",
                "template_type": "whatsapp_followup",
                "status": "pending"
            },
            {
                "day": 14,
                "channel": "WhatsApp",
                "action": f"New offer if price changed for {customer_name}",
                "template_type": "offer_refresh",
                "status": "pending"
            },
        ]

    # ─── Communication Logging ───────────────────────────────────────────────

    def log_communication(self, customer_name: str, channel: str,
                          template_type: str, content_preview: str = "") -> None:
        """Log a sent communication."""
        log = _load_json(COMM_LOG_FILE, [])
        log.append({
            "customer_name": customer_name,
            "channel": channel,
            "template_type": template_type,
            "content_preview": content_preview[:200],
            "sent_at": _now(),
            "status": "sent"
        })
        if len(log) > 5000:
            log = log[-5000:]
        _save_json(COMM_LOG_FILE, log)

    def get_communication_history(self, customer_name: str = None,
                                  limit: int = 50) -> list:
        """Get communication log, optionally filtered by customer."""
        log = _load_json(COMM_LOG_FILE, [])
        if customer_name:
            log = [l for l in log if l.get("customer_name") == customer_name]
        return log[-limit:]
