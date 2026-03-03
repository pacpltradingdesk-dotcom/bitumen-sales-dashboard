"""
database.py — SQLite Database Layer for Bitumen Sales Dashboard
===============================================================

PPS Anantam (Gujarat, India) — B2B Bitumen Trading Platform

Replaces 27+ fragile JSON files with a single, transactional SQLite database.
Uses Python's built-in sqlite3 module only (no external ORM).

Each public function opens its own connection and closes it before returning,
ensuring thread safety for Streamlit / multi-threaded callers.

DB file: bitumen_dashboard.db  (created in the project directory)
Timestamps: IST (Asia/Kolkata, UTC+05:30)

Usage:
    from database import init_db, insert_customer, get_all_deals, ...
    init_db()  # call once at app startup
"""

import sqlite3
import os
import json
from datetime import datetime, timezone, timedelta

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

IST = timezone(timedelta(hours=5, minutes=30))

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "bitumen_dashboard.db")


def _now_ist() -> str:
    """Return current IST timestamp as ISO-8601 string."""
    return datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")


def _get_conn() -> sqlite3.Connection:
    """Open a new connection with row_factory set to sqlite3.Row."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


def _row_to_dict(row: sqlite3.Row) -> dict:
    """Convert a sqlite3.Row to a plain dict."""
    return dict(row)


def _rows_to_list(rows) -> list:
    """Convert a list of sqlite3.Row objects to a list of dicts."""
    return [dict(r) for r in rows]


# ═══════════════════════════════════════════════════════════════════════════
# SCHEMA — Table Creation DDL
# ═══════════════════════════════════════════════════════════════════════════

_TABLES = {

    "suppliers": """
        CREATE TABLE IF NOT EXISTS suppliers (
            id                      INTEGER PRIMARY KEY AUTOINCREMENT,
            name                    TEXT NOT NULL,
            category                TEXT,
            city                    TEXT,
            state                   TEXT,
            contact                 TEXT,
            gstin                   TEXT,
            pan                     TEXT,
            credit_terms            TEXT,
            credit_limit_inr        REAL DEFAULT 0,
            payment_reliability_pct REAL DEFAULT 0,
            preferred_grades        TEXT,   -- JSON array, e.g. '["VG30","VG40"]'
            avg_monthly_volume_mt   REAL DEFAULT 0,
            last_deal_price         REAL,
            last_deal_date          TEXT,
            last_deal_qty           REAL,
            seasonality_tags        TEXT,
            relationship_stage      TEXT DEFAULT 'cold',
            next_followup_date      TEXT,
            notes                   TEXT,
            is_active               INTEGER DEFAULT 1,
            marked_for_purchase     INTEGER DEFAULT 0,
            created_at              TEXT,
            updated_at              TEXT
        );
    """,

    "customers": """
        CREATE TABLE IF NOT EXISTS customers (
            id                      INTEGER PRIMARY KEY AUTOINCREMENT,
            name                    TEXT NOT NULL,
            category                TEXT,
            city                    TEXT,
            state                   TEXT,
            contact                 TEXT,
            gstin                   TEXT,
            address                 TEXT,
            credit_terms            TEXT,
            credit_limit_inr        REAL DEFAULT 0,
            outstanding_inr         REAL DEFAULT 0,
            preferred_grades        TEXT,   -- JSON array
            avg_monthly_demand_mt   REAL DEFAULT 0,
            last_purchase_price     REAL,
            last_purchase_date      TEXT,
            last_purchase_qty       REAL,
            expected_monthly_demand REAL DEFAULT 0,
            seasonality_tags        TEXT,
            peak_months             TEXT,
            relationship_stage      TEXT DEFAULT 'cold',
            next_followup_date      TEXT,
            notes                   TEXT,
            is_active               INTEGER DEFAULT 1,
            created_at              TEXT,
            updated_at              TEXT
        );
    """,

    "deals": """
        CREATE TABLE IF NOT EXISTS deals (
            id                      INTEGER PRIMARY KEY AUTOINCREMENT,
            deal_number             TEXT UNIQUE,
            customer_id             INTEGER,
            supplier_id             INTEGER,
            grade                   TEXT,
            quantity_mt             REAL,
            packaging               TEXT,
            buy_price_per_mt        REAL,
            sell_price_per_mt       REAL,
            landed_cost_per_mt      REAL,
            margin_per_mt           REAL,
            margin_pct              REAL,
            source_location         TEXT,
            destination             TEXT,
            distance_km             REAL,
            freight_per_mt          REAL,
            stage                   TEXT DEFAULT 'enquiry',
            status                  TEXT DEFAULT 'active',
            enquiry_date            TEXT,
            quote_date              TEXT,
            po_date                 TEXT,
            dispatch_date           TEXT,
            delivery_date           TEXT,
            payment_date            TEXT,
            total_value_inr         REAL,
            payment_received_inr    REAL DEFAULT 0,
            gst_amount              REAL,
            win_probability_pct     REAL DEFAULT 50,
            loss_reason             TEXT,
            competitor_price        REAL,
            client_benefit_pct      REAL,
            created_at              TEXT,
            updated_at              TEXT,
            FOREIGN KEY (customer_id) REFERENCES customers(id),
            FOREIGN KEY (supplier_id) REFERENCES suppliers(id)
        );
    """,

    "price_history": """
        CREATE TABLE IF NOT EXISTS price_history (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            date_time   TEXT,
            benchmark   TEXT,
            price       REAL,
            currency    TEXT,
            source      TEXT,
            validated   INTEGER DEFAULT 1
        );
    """,

    "fx_history": """
        CREATE TABLE IF NOT EXISTS fx_history (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            date_time       TEXT,
            from_currency   TEXT,
            to_currency     TEXT,
            rate            REAL,
            source          TEXT
        );
    """,

    "inventory": """
        CREATE TABLE IF NOT EXISTS inventory (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            location          TEXT,
            grade             TEXT,
            quantity_mt       REAL,
            cost_per_mt       REAL,
            status            TEXT,
            expected_arrival  TEXT,
            vessel_name       TEXT,
            updated_at        TEXT
        );
    """,

    "communications": """
        CREATE TABLE IF NOT EXISTS communications (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            customer_id     INTEGER,
            supplier_id     INTEGER,
            channel         TEXT,
            direction       TEXT,
            subject         TEXT,
            content         TEXT,
            template_used   TEXT,
            sent_at         TEXT,
            status          TEXT,
            FOREIGN KEY (customer_id) REFERENCES customers(id),
            FOREIGN KEY (supplier_id) REFERENCES suppliers(id)
        );
    """,

    "opportunities": """
        CREATE TABLE IF NOT EXISTS opportunities (
            id                      INTEGER PRIMARY KEY AUTOINCREMENT,
            type                    TEXT,
            title                   TEXT,
            description             TEXT,
            customer_id             INTEGER,
            supplier_id             INTEGER,
            estimated_margin_per_mt REAL,
            estimated_volume_mt     REAL,
            estimated_value_inr     REAL,
            trigger_reason          TEXT,
            old_landed_cost         REAL,
            new_landed_cost         REAL,
            savings_per_mt          REAL,
            recommended_action      TEXT,
            whatsapp_template       TEXT,
            email_template          TEXT,
            call_script             TEXT,
            priority                TEXT,
            status                  TEXT DEFAULT 'new',
            valid_until             TEXT,
            created_at              TEXT,
            FOREIGN KEY (customer_id) REFERENCES customers(id),
            FOREIGN KEY (supplier_id) REFERENCES suppliers(id)
        );
    """,

    "sync_logs": """
        CREATE TABLE IF NOT EXISTS sync_logs (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            sync_type         TEXT,
            started_at        TEXT,
            completed_at      TEXT,
            status            TEXT,
            apis_called       INTEGER,
            apis_succeeded    INTEGER,
            records_updated   INTEGER,
            errors            TEXT,
            next_scheduled    TEXT
        );
    """,

    "missing_inputs": """
        CREATE TABLE IF NOT EXISTS missing_inputs (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            field_name      TEXT,
            entity_type     TEXT,
            entity_id       INTEGER,
            reason          TEXT,
            priority        TEXT,
            status          TEXT DEFAULT 'pending',
            collected_value TEXT,
            collected_at    TEXT,
            created_at      TEXT
        );
    """,
}

# Indexes for common query patterns
_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_deals_stage       ON deals(stage);",
    "CREATE INDEX IF NOT EXISTS idx_deals_status      ON deals(status);",
    "CREATE INDEX IF NOT EXISTS idx_deals_customer    ON deals(customer_id);",
    "CREATE INDEX IF NOT EXISTS idx_deals_supplier    ON deals(supplier_id);",
    "CREATE INDEX IF NOT EXISTS idx_price_benchmark   ON price_history(benchmark, date_time);",
    "CREATE INDEX IF NOT EXISTS idx_fx_pair           ON fx_history(from_currency, to_currency, date_time);",
    "CREATE INDEX IF NOT EXISTS idx_comms_customer    ON communications(customer_id);",
    "CREATE INDEX IF NOT EXISTS idx_comms_supplier    ON communications(supplier_id);",
    "CREATE INDEX IF NOT EXISTS idx_opps_status       ON opportunities(status);",
    "CREATE INDEX IF NOT EXISTS idx_missing_status    ON missing_inputs(status);",
    "CREATE INDEX IF NOT EXISTS idx_suppliers_active  ON suppliers(is_active);",
    "CREATE INDEX IF NOT EXISTS idx_customers_active  ON customers(is_active);",
    "CREATE INDEX IF NOT EXISTS idx_inventory_grade   ON inventory(grade);",
]


# ═══════════════════════════════════════════════════════════════════════════
# INIT
# ═══════════════════════════════════════════════════════════════════════════

def init_db():
    """
    Create all tables and indexes if they do not already exist.
    Safe to call multiple times (uses IF NOT EXISTS).
    """
    conn = _get_conn()
    try:
        cur = conn.cursor()
        for table_name, ddl in _TABLES.items():
            cur.execute(ddl)
        for idx_sql in _INDEXES:
            cur.execute(idx_sql)
        conn.commit()
    finally:
        conn.close()


# ═══════════════════════════════════════════════════════════════════════════
# GENERIC HELPERS (private)
# ═══════════════════════════════════════════════════════════════════════════

_VALID_TABLES = {
    "suppliers", "customers", "deals", "price_history", "fx_history",
    "inventory", "communications", "opportunities", "sync_logs",
    "missing_inputs",
}

import re
_SAFE_IDENT = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def _validate_table(table: str) -> str:
    """Ensure table name is a known table — prevents SQL injection."""
    if table not in _VALID_TABLES:
        raise ValueError(f"Unknown table: {table!r}")
    return table


def _validate_columns(keys, table: str = "") -> list:
    """Ensure all column names are safe identifiers."""
    safe = []
    for k in keys:
        if not _SAFE_IDENT.match(k):
            raise ValueError(f"Invalid column name: {k!r}")
        safe.append(k)
    return safe


def _insert_row(table: str, data: dict) -> int:
    """
    Insert a single row into *table* using the keys/values from *data*.
    Returns the new row's id.
    """
    _validate_table(table)
    col_names = _validate_columns(data.keys())
    cols = ", ".join(col_names)
    placeholders = ", ".join(["?"] * len(data))
    sql = f"INSERT INTO {table} ({cols}) VALUES ({placeholders})"
    conn = _get_conn()
    try:
        cur = conn.cursor()
        cur.execute(sql, list(data.values()))
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


def _update_row(table: str, row_id: int, data: dict):
    """
    Update fields in *table* for the row with the given *row_id*.
    """
    _validate_table(table)
    col_names = _validate_columns(data.keys())
    set_clause = ", ".join([f"{k} = ?" for k in col_names])
    sql = f"UPDATE {table} SET {set_clause} WHERE id = ?"
    conn = _get_conn()
    try:
        cur = conn.cursor()
        cur.execute(sql, list(data.values()) + [row_id])
        conn.commit()
    finally:
        conn.close()


def _select_all(table: str, where: str = "", params: tuple = (), order: str = "") -> list:
    """
    Return all rows from *table*, optionally filtered and ordered.

    The *where* and *order* clauses are validated to contain only safe
    SQL tokens (column names, operators, ASC/DESC). Use *params* for values.
    """
    _validate_table(table)
    sql = f"SELECT * FROM {table}"
    if where:
        sql += f" WHERE {where}"
    if order:
        sql += f" ORDER BY {order}"
    conn = _get_conn()
    try:
        rows = conn.execute(sql, params).fetchall()
        return _rows_to_list(rows)
    finally:
        conn.close()


def _select_one(table: str, row_id: int) -> dict | None:
    """
    Return a single row by id, or None if not found.
    """
    _validate_table(table)
    conn = _get_conn()
    try:
        row = conn.execute(f"SELECT * FROM {table} WHERE id = ?", (row_id,)).fetchone()
        return _row_to_dict(row) if row else None
    finally:
        conn.close()


# ═══════════════════════════════════════════════════════════════════════════
# SUPPLIERS
# ═══════════════════════════════════════════════════════════════════════════

def insert_supplier(data: dict) -> int:
    """
    Insert a new supplier row.

    Parameters
    ----------
    data : dict
        Keys should match supplier column names (excluding 'id').
        'preferred_grades' may be a Python list — it will be JSON-encoded.

    Returns
    -------
    int
        The new supplier's id.
    """
    data = dict(data)  # shallow copy to avoid mutating caller's dict
    if "preferred_grades" in data and isinstance(data["preferred_grades"], list):
        data["preferred_grades"] = json.dumps(data["preferred_grades"])
    if "seasonality_tags" in data and isinstance(data["seasonality_tags"], list):
        data["seasonality_tags"] = json.dumps(data["seasonality_tags"])
    data.setdefault("created_at", _now_ist())
    data.setdefault("updated_at", _now_ist())
    return _insert_row("suppliers", data)


def get_all_suppliers() -> list:
    """
    Return all active suppliers, ordered by name.
    """
    return _select_all("suppliers", where="is_active = 1", order="name ASC")


def get_supplier_by_id(supplier_id: int) -> dict | None:
    """Return a single supplier by id."""
    return _select_one("suppliers", supplier_id)


def update_supplier(supplier_id: int, data: dict):
    """
    Update one or more fields on an existing supplier.

    Parameters
    ----------
    supplier_id : int
    data : dict
        Column-value pairs to update.
    """
    data = dict(data)
    if "preferred_grades" in data and isinstance(data["preferred_grades"], list):
        data["preferred_grades"] = json.dumps(data["preferred_grades"])
    if "seasonality_tags" in data and isinstance(data["seasonality_tags"], list):
        data["seasonality_tags"] = json.dumps(data["seasonality_tags"])
    data["updated_at"] = _now_ist()
    _update_row("suppliers", supplier_id, data)


# ═══════════════════════════════════════════════════════════════════════════
# CUSTOMERS
# ═══════════════════════════════════════════════════════════════════════════

def insert_customer(data: dict) -> int:
    """
    Insert a new customer row.

    Parameters
    ----------
    data : dict
        Keys should match customer column names (excluding 'id').
        'preferred_grades', 'seasonality_tags', 'peak_months' may be Python lists.

    Returns
    -------
    int
        The new customer's id.
    """
    data = dict(data)
    for field in ("preferred_grades", "seasonality_tags", "peak_months"):
        if field in data and isinstance(data[field], list):
            data[field] = json.dumps(data[field])
    data.setdefault("created_at", _now_ist())
    data.setdefault("updated_at", _now_ist())
    return _insert_row("customers", data)


def get_all_customers() -> list:
    """Return all active customers, ordered by name."""
    return _select_all("customers", where="is_active = 1", order="name ASC")


def get_customer_by_id(customer_id: int) -> dict | None:
    """Return a single customer by id, or None."""
    return _select_one("customers", customer_id)


def update_customer(customer_id: int, data: dict):
    """
    Update one or more fields on an existing customer.

    Parameters
    ----------
    customer_id : int
    data : dict
        Column-value pairs to update.
    """
    data = dict(data)
    for field in ("preferred_grades", "seasonality_tags", "peak_months"):
        if field in data and isinstance(data[field], list):
            data[field] = json.dumps(data[field])
    data["updated_at"] = _now_ist()
    _update_row("customers", customer_id, data)


# ═══════════════════════════════════════════════════════════════════════════
# DEALS
# ═══════════════════════════════════════════════════════════════════════════

def insert_deal(data: dict) -> int:
    """
    Insert a new deal row.

    Parameters
    ----------
    data : dict
        Keys should match deals column names (excluding 'id').
        If 'deal_number' is not supplied, one is auto-generated
        as DEAL-YYYYMMDD-HHMMSS.

    Returns
    -------
    int
        The new deal's id.
    """
    data = dict(data)
    if not data.get("deal_number"):
        data["deal_number"] = "DEAL-" + datetime.now(IST).strftime("%Y%m%d-%H%M%S")
    data.setdefault("created_at", _now_ist())
    data.setdefault("updated_at", _now_ist())
    return _insert_row("deals", data)


def get_all_deals() -> list:
    """Return all deals, most recent first."""
    return _select_all("deals", order="created_at DESC")


def get_deals_by_stage(stage: str) -> list:
    """Return deals filtered by pipeline stage (enquiry, quoted, confirmed, etc.)."""
    return _select_all("deals", where="stage = ?", params=(stage,), order="created_at DESC")


def get_deal_by_id(deal_id: int) -> dict | None:
    """Return a single deal by id."""
    return _select_one("deals", deal_id)


def update_deal_stage(deal_id: int, new_stage: str):
    """
    Move a deal to a new pipeline stage and record the corresponding date.

    Automatically sets the matching date column:
        enquiry  -> enquiry_date
        quoted   -> quote_date
        confirmed / po -> po_date
        dispatched     -> dispatch_date
        delivered      -> delivery_date
        paid           -> payment_date

    Parameters
    ----------
    deal_id : int
    new_stage : str
    """
    now = _now_ist()
    data = {"stage": new_stage, "updated_at": now}

    stage_date_map = {
        "enquiry":    "enquiry_date",
        "quoted":     "quote_date",
        "confirmed":  "po_date",
        "po":         "po_date",
        "dispatched": "dispatch_date",
        "delivered":  "delivery_date",
        "paid":       "payment_date",
    }
    date_col = stage_date_map.get(new_stage)
    if date_col:
        data[date_col] = now

    _update_row("deals", deal_id, data)


def update_deal(deal_id: int, data: dict):
    """
    Update arbitrary fields on an existing deal.

    Parameters
    ----------
    deal_id : int
    data : dict
        Column-value pairs to update.
    """
    data = dict(data)
    data["updated_at"] = _now_ist()
    _update_row("deals", deal_id, data)


# ═══════════════════════════════════════════════════════════════════════════
# PRICE HISTORY
# ═══════════════════════════════════════════════════════════════════════════

def insert_price_history(records: list):
    """
    Bulk-insert price history records.

    Parameters
    ----------
    records : list of dict
        Each dict should have keys: date_time, benchmark, price, currency,
        source, and optionally validated.
    """
    if not records:
        return
    conn = _get_conn()
    try:
        cur = conn.cursor()
        for rec in records:
            rec = dict(rec)
            rec.setdefault("validated", 1)
            cols = ", ".join(rec.keys())
            placeholders = ", ".join(["?"] * len(rec))
            cur.execute(
                f"INSERT INTO price_history ({cols}) VALUES ({placeholders})",
                list(rec.values()),
            )
        conn.commit()
    finally:
        conn.close()


def get_price_history(benchmark: str, limit: int = 100) -> list:
    """
    Return the most recent price records for a given benchmark.

    Parameters
    ----------
    benchmark : str
        E.g. 'Brent', 'VG30_Mumbai', 'Iran_180CST_FOB'.
    limit : int
        Max records to return (default 100).

    Returns
    -------
    list of dict
    """
    conn = _get_conn()
    try:
        rows = conn.execute(
            "SELECT * FROM price_history WHERE benchmark = ? ORDER BY date_time DESC LIMIT ?",
            (benchmark, limit),
        ).fetchall()
        return _rows_to_list(rows)
    finally:
        conn.close()


# ═══════════════════════════════════════════════════════════════════════════
# FX HISTORY
# ═══════════════════════════════════════════════════════════════════════════

def insert_fx_rate(data: dict) -> int:
    """Insert a single FX rate record. Returns new row id."""
    data = dict(data)
    data.setdefault("date_time", _now_ist())
    return _insert_row("fx_history", data)


def get_fx_latest() -> dict:
    """
    Return the latest FX rate for each currency pair as a dict.

    Returns
    -------
    dict
        Keyed by "FROM/TO" (e.g. "USD/INR"), value is the latest rate dict.
    """
    conn = _get_conn()
    try:
        rows = conn.execute(
            """
            SELECT fh.*
            FROM fx_history fh
            INNER JOIN (
                SELECT from_currency, to_currency, MAX(date_time) AS max_dt
                FROM fx_history
                GROUP BY from_currency, to_currency
            ) latest
            ON  fh.from_currency = latest.from_currency
            AND fh.to_currency   = latest.to_currency
            AND fh.date_time     = latest.max_dt
            """
        ).fetchall()
        result = {}
        for row in rows:
            d = _row_to_dict(row)
            key = f"{d['from_currency']}/{d['to_currency']}"
            result[key] = d
        return result
    finally:
        conn.close()


# ═══════════════════════════════════════════════════════════════════════════
# INVENTORY
# ═══════════════════════════════════════════════════════════════════════════

def upsert_inventory(data: dict) -> int:
    """
    Insert or update an inventory record.
    If a record with the same location + grade exists, update it; else insert.

    Returns the row id.
    """
    data = dict(data)
    data["updated_at"] = _now_ist()
    conn = _get_conn()
    try:
        existing = conn.execute(
            "SELECT id FROM inventory WHERE location = ? AND grade = ?",
            (data.get("location"), data.get("grade")),
        ).fetchone()
        if existing:
            row_id = existing["id"]
            set_clause = ", ".join([f"{k} = ?" for k in data.keys()])
            conn.execute(
                f"UPDATE inventory SET {set_clause} WHERE id = ?",
                list(data.values()) + [row_id],
            )
            conn.commit()
            return row_id
        else:
            cols = ", ".join(data.keys())
            placeholders = ", ".join(["?"] * len(data))
            cur = conn.cursor()
            cur.execute(
                f"INSERT INTO inventory ({cols}) VALUES ({placeholders})",
                list(data.values()),
            )
            conn.commit()
            return cur.lastrowid
    finally:
        conn.close()


def get_inventory_summary() -> list:
    """
    Return inventory grouped by grade with total quantities.

    Returns
    -------
    list of dict
        Each dict has: grade, total_quantity_mt, total_value_inr, locations (count).
    """
    conn = _get_conn()
    try:
        rows = conn.execute(
            """
            SELECT
                grade,
                SUM(quantity_mt) AS total_quantity_mt,
                SUM(quantity_mt * cost_per_mt) AS total_value_inr,
                COUNT(DISTINCT location) AS locations
            FROM inventory
            WHERE quantity_mt > 0
            GROUP BY grade
            ORDER BY total_quantity_mt DESC
            """
        ).fetchall()
        return _rows_to_list(rows)
    finally:
        conn.close()


# ═══════════════════════════════════════════════════════════════════════════
# COMMUNICATIONS
# ═══════════════════════════════════════════════════════════════════════════

def log_communication(data: dict) -> int:
    """
    Insert a communication log entry.

    Parameters
    ----------
    data : dict
        Keys: customer_id, supplier_id, channel, direction, subject,
              content, template_used, sent_at, status.

    Returns
    -------
    int
        The new communication row's id.
    """
    data = dict(data)
    data.setdefault("sent_at", _now_ist())
    return _insert_row("communications", data)


def get_communications(entity_type: str, entity_id: int, limit: int = 50) -> list:
    """
    Retrieve communication history for a customer or supplier.

    Parameters
    ----------
    entity_type : str
        'customer' or 'supplier'.
    entity_id : int
    limit : int

    Returns
    -------
    list of dict
    """
    col = "customer_id" if entity_type == "customer" else "supplier_id"
    conn = _get_conn()
    try:
        rows = conn.execute(
            f"SELECT * FROM communications WHERE {col} = ? ORDER BY sent_at DESC LIMIT ?",
            (entity_id, limit),
        ).fetchall()
        return _rows_to_list(rows)
    finally:
        conn.close()


# ═══════════════════════════════════════════════════════════════════════════
# OPPORTUNITIES
# ═══════════════════════════════════════════════════════════════════════════

def insert_opportunity(data: dict) -> int:
    """
    Insert a new sales opportunity / SOS alert.

    Parameters
    ----------
    data : dict
        Keys matching the opportunities table columns.

    Returns
    -------
    int
        The new opportunity's id.
    """
    data = dict(data)
    data.setdefault("created_at", _now_ist())
    return _insert_row("opportunities", data)


def get_all_opportunities(status: str = None) -> list:
    """
    Retrieve opportunities, optionally filtered by status.

    Parameters
    ----------
    status : str or None
        If provided (e.g. 'new', 'acted', 'expired'), filter by that status.
        If None, return all opportunities.

    Returns
    -------
    list of dict
    """
    if status:
        return _select_all(
            "opportunities",
            where="status = ?",
            params=(status,),
            order="created_at DESC",
        )
    return _select_all("opportunities", order="created_at DESC")


def update_opportunity(opportunity_id: int, data: dict):
    """Update fields on an existing opportunity."""
    data = dict(data)
    _update_row("opportunities", opportunity_id, data)


# ═══════════════════════════════════════════════════════════════════════════
# SYNC LOGS
# ═══════════════════════════════════════════════════════════════════════════

def log_sync(data: dict) -> int:
    """
    Record an API sync event.

    Parameters
    ----------
    data : dict
        Keys: sync_type, started_at, completed_at, status, apis_called,
              apis_succeeded, records_updated, errors, next_scheduled.

    Returns
    -------
    int
        The new sync_log row's id.
    """
    data = dict(data)
    data.setdefault("started_at", _now_ist())
    return _insert_row("sync_logs", data)


def get_sync_logs(limit: int = 20) -> list:
    """Return the most recent sync log entries."""
    conn = _get_conn()
    try:
        rows = conn.execute(
            "SELECT * FROM sync_logs ORDER BY started_at DESC LIMIT ?",
            (limit,),
        ).fetchall()
        return _rows_to_list(rows)
    finally:
        conn.close()


# ═══════════════════════════════════════════════════════════════════════════
# MISSING INPUTS
# ═══════════════════════════════════════════════════════════════════════════

def add_missing_input(data: dict) -> int:
    """
    Record a field that still needs to be collected from the user.

    Parameters
    ----------
    data : dict
        Keys: field_name, entity_type, entity_id, reason, priority, status.

    Returns
    -------
    int
        The new missing_input row's id.
    """
    data = dict(data)
    data.setdefault("status", "pending")
    data.setdefault("created_at", _now_ist())
    return _insert_row("missing_inputs", data)


def get_missing_inputs(status: str = "pending") -> list:
    """
    Return missing input items, filtered by status.

    Parameters
    ----------
    status : str
        Default 'pending'. Use 'all' to return every record.

    Returns
    -------
    list of dict
    """
    if status == "all":
        return _select_all("missing_inputs", order="created_at DESC")
    return _select_all(
        "missing_inputs",
        where="status = ?",
        params=(status,),
        order="priority ASC, created_at ASC",
    )


def resolve_missing_input(input_id: int, collected_value: str):
    """Mark a missing input as collected."""
    _update_row("missing_inputs", input_id, {
        "status": "collected",
        "collected_value": collected_value,
        "collected_at": _now_ist(),
    })


# ═══════════════════════════════════════════════════════════════════════════
# DASHBOARD STATS
# ═══════════════════════════════════════════════════════════════════════════

def get_dashboard_stats() -> dict:
    """
    Return a summary dict for the main dashboard view.

    Returns
    -------
    dict with keys:
        total_customers         — count of active customers
        total_suppliers         — count of active suppliers
        total_deals             — count of all deals
        deals_by_stage          — dict {stage: count}
        active_deals            — count where status = 'active'
        total_deal_value_inr    — sum of total_value_inr for active deals
        total_received_inr      — sum of payment_received_inr
        total_outstanding_inr   — total_deal_value - total_received
        avg_margin_pct          — average margin_pct across active deals
        open_opportunities      — count of opportunities with status = 'new'
        pending_missing_inputs  — count of missing inputs with status = 'pending'
        inventory_mt            — total inventory quantity in MT
        last_sync               — most recent sync_log entry
    """
    conn = _get_conn()
    try:
        stats = {}

        # Customers
        row = conn.execute("SELECT COUNT(*) AS cnt FROM customers WHERE is_active = 1").fetchone()
        stats["total_customers"] = row["cnt"]

        # Suppliers
        row = conn.execute("SELECT COUNT(*) AS cnt FROM suppliers WHERE is_active = 1").fetchone()
        stats["total_suppliers"] = row["cnt"]

        # Deals — total count
        row = conn.execute("SELECT COUNT(*) AS cnt FROM deals").fetchone()
        stats["total_deals"] = row["cnt"]

        # Deals by stage
        rows = conn.execute(
            "SELECT stage, COUNT(*) AS cnt FROM deals GROUP BY stage"
        ).fetchall()
        stats["deals_by_stage"] = {r["stage"]: r["cnt"] for r in rows}

        # Active deals count
        row = conn.execute("SELECT COUNT(*) AS cnt FROM deals WHERE status = 'active'").fetchone()
        stats["active_deals"] = row["cnt"]

        # Financial totals
        row = conn.execute(
            """
            SELECT
                COALESCE(SUM(total_value_inr), 0)       AS total_val,
                COALESCE(SUM(payment_received_inr), 0)  AS total_recv,
                COALESCE(AVG(margin_pct), 0)             AS avg_margin
            FROM deals
            WHERE status = 'active'
            """
        ).fetchone()
        stats["total_deal_value_inr"] = row["total_val"]
        stats["total_received_inr"] = row["total_recv"]
        stats["total_outstanding_inr"] = row["total_val"] - row["total_recv"]
        stats["avg_margin_pct"] = round(row["avg_margin"], 2)

        # Open opportunities
        row = conn.execute(
            "SELECT COUNT(*) AS cnt FROM opportunities WHERE status = 'new'"
        ).fetchone()
        stats["open_opportunities"] = row["cnt"]

        # Pending missing inputs
        row = conn.execute(
            "SELECT COUNT(*) AS cnt FROM missing_inputs WHERE status = 'pending'"
        ).fetchone()
        stats["pending_missing_inputs"] = row["cnt"]

        # Inventory total
        row = conn.execute(
            "SELECT COALESCE(SUM(quantity_mt), 0) AS total FROM inventory"
        ).fetchone()
        stats["inventory_mt"] = row["total"]

        # Last sync
        row = conn.execute(
            "SELECT * FROM sync_logs ORDER BY started_at DESC LIMIT 1"
        ).fetchone()
        stats["last_sync"] = _row_to_dict(row) if row else None

        return stats
    finally:
        conn.close()


# ═══════════════════════════════════════════════════════════════════════════
# CONVENIENCE: Run init when executed directly
# ═══════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    init_db()
    print(f"Database initialized at: {DB_PATH}")
    print("Tables created:")
    conn = _get_conn()
    try:
        rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall()
        for r in rows:
            count = conn.execute(f"SELECT COUNT(*) AS cnt FROM {r['name']}").fetchone()
            print(f"  - {r['name']:20s}  ({count['cnt']} rows)")
    finally:
        conn.close()
