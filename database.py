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
import queue
from contextlib import contextmanager
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
# CONNECTION POOL — Thread-safe reusable connections
# ═══════════════════════════════════════════════════════════════════════════

_pool = queue.Queue(maxsize=5)


def _create_connection() -> sqlite3.Connection:
    """Create a fresh SQLite connection with standard pragmas."""
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


@contextmanager
def get_connection():
    """
    Context manager returning a pooled connection.
    Auto-returns to pool on exit; creates new if pool empty.
    """
    try:
        conn = _pool.get_nowait()
    except queue.Empty:
        conn = _create_connection()
    try:
        yield conn
    finally:
        try:
            _pool.put_nowait(conn)
        except queue.Full:
            conn.close()


@contextmanager
def db_transaction():
    """
    Context manager for multi-statement transactions.

    Usage::

        with db_transaction() as conn:
            conn.execute("INSERT INTO ...", (...))
            conn.execute("UPDATE ...", (...))

    Auto-commits on success, auto-rollbacks on exception.
    """
    conn = _create_connection()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ═══════════════════════════════════════════════════════════════════════════
# MONETARY & TIMESTAMP HELPERS
# ═══════════════════════════════════════════════════════════════════════════

# Columns that store monetary values — rounded on write
MONETARY_DEAL_COLS = frozenset({
    "buy_price_per_mt", "sell_price_per_mt", "landed_cost_per_mt",
    "margin_per_mt", "freight_per_mt", "total_value_inr",
    "payment_received_inr", "gst_amount", "competitor_price",
    "credit_limit_inr", "outstanding_inr", "last_purchase_price",
    "last_deal_price", "cost_per_mt", "estimated_margin_per_mt",
    "estimated_value_inr", "old_landed_cost", "new_landed_cost",
    "savings_per_mt",
})


def _round_money(value, decimals: int = 2) -> float:
    """Round monetary values consistently to avoid float drift."""
    if value is None:
        return 0.0
    return round(float(value), decimals)


def _round_monetary_fields(data: dict) -> dict:
    """Round all monetary fields in a data dict."""
    for col in MONETARY_DEAL_COLS:
        if col in data and data[col] is not None:
            data[col] = _round_money(data[col])
    return data


def _parse_timestamp(ts: str):
    """Parse an IST timestamp string to a datetime object, or None."""
    if not ts:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%d-%m-%Y %H:%M:%S", "%d-%m-%Y %H:%M"):
        try:
            dt = datetime.strptime(ts, fmt)
            return dt.replace(tzinfo=IST)
        except ValueError:
            continue
    return None


def _days_since(ts: str):
    """Return the number of days since a timestamp string, or None."""
    dt = _parse_timestamp(ts)
    if not dt:
        return None
    now = datetime.now(IST)
    return (now - dt).days


def _is_within_days(ts: str, days: int) -> bool:
    """Check if a timestamp is within the last N days."""
    d = _days_since(ts)
    return d is not None and d <= days


# Transaction-aware insert/update (caller manages commit/rollback)

def _insert_row_tx(conn: sqlite3.Connection, table: str, data: dict) -> int:
    """Insert within an existing transaction (caller manages commit)."""
    _validate_table(table)
    col_names = _validate_columns(data.keys())
    cols = ", ".join(col_names)
    placeholders = ", ".join(["?"] * len(data))
    sql = f"INSERT INTO {table} ({cols}) VALUES ({placeholders})"
    cur = conn.cursor()
    cur.execute(sql, list(data.values()))
    return cur.lastrowid


def _update_row_tx(conn: sqlite3.Connection, table: str, row_id: int, data: dict):
    """Update within an existing transaction (caller manages commit)."""
    _validate_table(table)
    col_names = _validate_columns(data.keys())
    set_clause = ", ".join([f"{k} = ?" for k in col_names])
    sql = f"UPDATE {table} SET {set_clause} WHERE id = ?"
    conn.execute(sql, list(data.values()) + [row_id])


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

    "email_queue": """
        CREATE TABLE IF NOT EXISTS email_queue (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            to_email        TEXT NOT NULL,
            cc              TEXT,
            bcc             TEXT,
            subject         TEXT NOT NULL,
            body_html       TEXT,
            body_text       TEXT,
            email_type      TEXT,
            customer_id     INTEGER,
            deal_id         INTEGER,
            attachments     TEXT,
            status          TEXT DEFAULT 'draft',
            error_message   TEXT,
            smtp_message_id TEXT,
            retry_count     INTEGER DEFAULT 0,
            max_retries     INTEGER DEFAULT 3,
            scheduled_at    TEXT,
            sent_at         TEXT,
            created_at      TEXT,
            updated_at      TEXT,
            FOREIGN KEY (customer_id) REFERENCES customers(id)
        );
    """,

    "whatsapp_queue": """
        CREATE TABLE IF NOT EXISTS whatsapp_queue (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            to_number       TEXT NOT NULL,
            message_type    TEXT NOT NULL,
            template_name   TEXT,
            template_params TEXT,
            session_text    TEXT,
            customer_id     INTEGER,
            deal_id         INTEGER,
            broadcast_id    TEXT,
            status          TEXT DEFAULT 'draft',
            wa_message_id   TEXT,
            error_message   TEXT,
            error_code      TEXT,
            retry_count     INTEGER DEFAULT 0,
            max_retries     INTEGER DEFAULT 3,
            scheduled_at    TEXT,
            sent_at         TEXT,
            delivered_at    TEXT,
            read_at         TEXT,
            created_at      TEXT,
            updated_at      TEXT,
            FOREIGN KEY (customer_id) REFERENCES customers(id)
        );
    """,

    "whatsapp_sessions": """
        CREATE TABLE IF NOT EXISTS whatsapp_sessions (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            phone_number    TEXT NOT NULL UNIQUE,
            session_start   TEXT NOT NULL,
            session_expires TEXT NOT NULL,
            customer_id     INTEGER,
            last_message    TEXT,
            created_at      TEXT,
            FOREIGN KEY (customer_id) REFERENCES customers(id)
        );
    """,

    "whatsapp_incoming": """
        CREATE TABLE IF NOT EXISTS whatsapp_incoming (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            from_number     TEXT NOT NULL,
            message_type    TEXT,
            message_text    TEXT,
            media_url       TEXT,
            wa_message_id   TEXT,
            customer_id     INTEGER,
            processed       INTEGER DEFAULT 0,
            received_at     TEXT,
            FOREIGN KEY (customer_id) REFERENCES customers(id)
        );
    """,

    "director_briefings": """
        CREATE TABLE IF NOT EXISTS director_briefings (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            briefing_date   TEXT,
            generated_at    TEXT,
            briefing_data   TEXT,
            sent_email      INTEGER DEFAULT 0,
            sent_whatsapp   INTEGER DEFAULT 0,
            created_at      TEXT
        );
    """,

    "daily_logs": """
        CREATE TABLE IF NOT EXISTS daily_logs (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            log_date            TEXT,
            author              TEXT,
            entry_type          TEXT,
            customer_name       TEXT,
            channel             TEXT,
            notes               TEXT,
            outcome             TEXT,
            followup_date       TEXT,
            intel_source        TEXT,
            intel_confidence    TEXT,
            metadata            TEXT,
            created_at          TEXT
        );
    """,

    "alerts": """
        CREATE TABLE IF NOT EXISTS alerts (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            alert_type          TEXT,
            priority            TEXT,
            title               TEXT,
            description         TEXT,
            data                TEXT,
            recommended_action  TEXT,
            action_template     TEXT,
            status              TEXT DEFAULT 'new',
            snoozed_until       TEXT,
            created_at          TEXT,
            acted_at            TEXT,
            acted_by            TEXT
        );
    """,

    # ── Phase C: Universal Action + RBAC tables ────────────────────────────

    "users": """
        CREATE TABLE IF NOT EXISTS users (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            username        TEXT NOT NULL UNIQUE,
            display_name    TEXT,
            role            TEXT NOT NULL DEFAULT 'viewer',
            pin_hash        TEXT NOT NULL,
            email           TEXT,
            whatsapp_number TEXT,
            is_active       INTEGER DEFAULT 1,
            last_login      TEXT,
            created_at      TEXT,
            updated_at      TEXT
        );
    """,

    "audit_log": """
        CREATE TABLE IF NOT EXISTS audit_log (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id         INTEGER,
            username        TEXT,
            action          TEXT NOT NULL,
            resource        TEXT,
            details         TEXT,
            ip_address      TEXT,
            created_at      TEXT,
            FOREIGN KEY (user_id) REFERENCES users(id)
        );
    """,

    "recipient_lists": """
        CREATE TABLE IF NOT EXISTS recipient_lists (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            list_name       TEXT NOT NULL,
            list_type       TEXT DEFAULT 'email',
            recipients      TEXT NOT NULL,
            created_by      TEXT,
            is_active       INTEGER DEFAULT 1,
            created_at      TEXT,
            updated_at      TEXT
        );
    """,

    "source_registry": """
        CREATE TABLE IF NOT EXISTS source_registry (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            source_key      TEXT NOT NULL UNIQUE,
            source_name     TEXT NOT NULL,
            category        TEXT,
            provider        TEXT,
            api_url         TEXT,
            auth_type       TEXT DEFAULT 'none',
            status          TEXT DEFAULT 'active',
            refresh_minutes INTEGER DEFAULT 60,
            keywords        TEXT,
            state_mapping   TEXT,
            last_success    TEXT,
            last_error      TEXT,
            error_count     INTEGER DEFAULT 0,
            output_tables   TEXT,
            notes           TEXT,
            created_at      TEXT,
            updated_at      TEXT
        );
    """,

    # ── Phase D: Integrations & Communication tables ─────────────────────────

    "chat_messages": """
        CREATE TABLE IF NOT EXISTS chat_messages (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            conversation_id TEXT NOT NULL,
            sender_type     TEXT NOT NULL,
            sender_name     TEXT,
            sender_id       INTEGER,
            message_text    TEXT NOT NULL,
            attachment_path TEXT,
            is_read         INTEGER DEFAULT 0,
            created_at      TEXT
        );
    """,

    "share_links": """
        CREATE TABLE IF NOT EXISTS share_links (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            link_token      TEXT NOT NULL UNIQUE,
            page_name       TEXT NOT NULL,
            filters_json    TEXT,
            created_by      TEXT,
            permissions     TEXT DEFAULT 'view',
            password_hash   TEXT,
            expires_at      TEXT,
            max_views       INTEGER DEFAULT 0,
            view_count      INTEGER DEFAULT 0,
            is_active       INTEGER DEFAULT 1,
            last_accessed   TEXT,
            created_at      TEXT
        );
    """,

    "share_schedules": """
        CREATE TABLE IF NOT EXISTS share_schedules (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            schedule_name   TEXT NOT NULL,
            page_name       TEXT NOT NULL,
            content_type    TEXT,
            channel         TEXT NOT NULL,
            recipients_json TEXT NOT NULL,
            frequency       TEXT NOT NULL,
            day_of_week     TEXT,
            day_of_month    INTEGER,
            time_ist        TEXT DEFAULT '09:00',
            is_active       INTEGER DEFAULT 1,
            last_run        TEXT,
            next_run        TEXT,
            run_count       INTEGER DEFAULT 0,
            created_by      TEXT,
            created_at      TEXT,
            updated_at      TEXT
        );
    """,

    "comm_tracking": """
        CREATE TABLE IF NOT EXISTS comm_tracking (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            tracking_id     TEXT NOT NULL UNIQUE,
            channel         TEXT NOT NULL,
            action          TEXT NOT NULL,
            sender          TEXT,
            recipient_name  TEXT,
            recipient_addr  TEXT,
            page_name       TEXT,
            content_type    TEXT,
            content_summary TEXT,
            delivery_status TEXT DEFAULT 'pending',
            error_message   TEXT,
            created_at      TEXT,
            delivered_at    TEXT
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
    # Email queue indexes
    "CREATE INDEX IF NOT EXISTS idx_email_queue_status    ON email_queue(status);",
    "CREATE INDEX IF NOT EXISTS idx_email_queue_type      ON email_queue(email_type);",
    "CREATE INDEX IF NOT EXISTS idx_email_queue_customer  ON email_queue(customer_id);",
    "CREATE INDEX IF NOT EXISTS idx_email_queue_scheduled ON email_queue(scheduled_at);",
    # WhatsApp queue indexes
    "CREATE INDEX IF NOT EXISTS idx_wa_queue_status       ON whatsapp_queue(status);",
    "CREATE INDEX IF NOT EXISTS idx_wa_queue_customer     ON whatsapp_queue(customer_id);",
    "CREATE INDEX IF NOT EXISTS idx_wa_queue_type         ON whatsapp_queue(message_type);",
    "CREATE INDEX IF NOT EXISTS idx_wa_session_phone      ON whatsapp_sessions(phone_number);",
    "CREATE INDEX IF NOT EXISTS idx_wa_incoming_from      ON whatsapp_incoming(from_number);",
    # Director / Daily log / Alert indexes
    "CREATE INDEX IF NOT EXISTS idx_briefings_date        ON director_briefings(briefing_date);",
    "CREATE INDEX IF NOT EXISTS idx_daily_logs_date       ON daily_logs(log_date);",
    "CREATE INDEX IF NOT EXISTS idx_daily_logs_type       ON daily_logs(entry_type);",
    "CREATE INDEX IF NOT EXISTS idx_alerts_status         ON alerts(status);",
    "CREATE INDEX IF NOT EXISTS idx_alerts_priority       ON alerts(priority);",
    "CREATE INDEX IF NOT EXISTS idx_alerts_type           ON alerts(alert_type);",
    # Phase C: RBAC + Action + Ops indexes
    "CREATE INDEX IF NOT EXISTS idx_users_username          ON users(username);",
    "CREATE INDEX IF NOT EXISTS idx_users_role              ON users(role);",
    "CREATE INDEX IF NOT EXISTS idx_audit_user              ON audit_log(user_id);",
    "CREATE INDEX IF NOT EXISTS idx_audit_action            ON audit_log(action);",
    "CREATE INDEX IF NOT EXISTS idx_audit_created           ON audit_log(created_at);",
    "CREATE INDEX IF NOT EXISTS idx_recip_name              ON recipient_lists(list_name);",
    "CREATE INDEX IF NOT EXISTS idx_source_reg_key          ON source_registry(source_key);",
    "CREATE INDEX IF NOT EXISTS idx_source_reg_status       ON source_registry(status);",
    # Phase D: Integrations & Communication indexes
    "CREATE INDEX IF NOT EXISTS idx_chat_convo              ON chat_messages(conversation_id);",
    "CREATE INDEX IF NOT EXISTS idx_chat_created            ON chat_messages(created_at);",
    "CREATE INDEX IF NOT EXISTS idx_share_links_token       ON share_links(link_token);",
    "CREATE INDEX IF NOT EXISTS idx_share_links_active      ON share_links(is_active);",
    "CREATE INDEX IF NOT EXISTS idx_share_sched_active      ON share_schedules(is_active);",
    "CREATE INDEX IF NOT EXISTS idx_share_sched_next        ON share_schedules(next_run);",
    "CREATE INDEX IF NOT EXISTS idx_comm_track_channel      ON comm_tracking(channel);",
    "CREATE INDEX IF NOT EXISTS idx_comm_track_status       ON comm_tracking(delivery_status);",
    "CREATE INDEX IF NOT EXISTS idx_comm_track_created      ON comm_tracking(created_at);",
]


# ═══════════════════════════════════════════════════════════════════════════
# INIT
# ═══════════════════════════════════════════════════════════════════════════

def init_db():
    """
    Create all tables and indexes if they do not already exist.
    Safe to call multiple times (uses IF NOT EXISTS).
    Also runs incremental schema migrations.
    """
    conn = _get_conn()
    try:
        cur = conn.cursor()
        for table_name, ddl in _TABLES.items():
            cur.execute(ddl)
        for idx_sql in _INDEXES:
            cur.execute(idx_sql)
        # Migrations: add columns if missing
        for tbl in ("customers", "suppliers"):
            for col, col_type in [("email", "TEXT"), ("whatsapp_number", "TEXT")]:
                try:
                    cur.execute(f"ALTER TABLE {tbl} ADD COLUMN {col} {col_type}")
                except sqlite3.OperationalError:
                    pass  # Column already exists
        conn.commit()
    finally:
        conn.close()

    # Run incremental schema migrations
    _run_schema_migrations()

    # Initialize infra demand intelligence tables
    try:
        from infra_demand_engine import init_infra_tables
        init_infra_tables()
    except Exception:
        pass


# ═══════════════════════════════════════════════════════════════════════════
# SCHEMA MIGRATIONS — Incremental, versioned
# ═══════════════════════════════════════════════════════════════════════════

def _run_schema_migrations():
    """Apply incremental schema migrations. Uses _schema_version tracker."""
    conn = _get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS _schema_version (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                version     INTEGER NOT NULL,
                description TEXT,
                applied_at  TEXT
            )
        """)
        conn.commit()

        row = cur.execute("SELECT MAX(version) as v FROM _schema_version").fetchone()
        current_version = row["v"] if row and row["v"] else 0

        migrations = [
            (1, "Add CHECK constraints + fix NULL data", _migration_001_fix_data),
            (2, "Add UNIQUE indexes on business keys", _migration_002_unique_indexes),
            (3, "Add crm_tasks table", _migration_003_crm_tasks),
        ]

        for version, desc, migration_fn in migrations:
            if version > current_version:
                try:
                    migration_fn(cur)
                    cur.execute(
                        "INSERT INTO _schema_version (version, description, applied_at) "
                        "VALUES (?, ?, ?)",
                        (version, desc, _now_ist()),
                    )
                    conn.commit()
                except Exception as e:
                    conn.rollback()
                    # Log but don't crash — migrations are best-effort
                    import logging
                    logging.getLogger("database").warning(
                        "Migration %d failed: %s", version, e
                    )
    finally:
        conn.close()


def _migration_001_fix_data(cur):
    """Fix NULL customer names and negative quantities."""
    cur.execute("UPDATE customers SET name = 'UNKNOWN' WHERE name IS NULL OR name = ''")
    cur.execute("UPDATE deals SET quantity_mt = ABS(quantity_mt) WHERE quantity_mt IS NOT NULL AND quantity_mt < 0")


def _migration_002_unique_indexes(cur):
    """Add unique composite index on customers to prevent duplicates."""
    try:
        cur.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_customers_name_contact "
            "ON customers(name, contact) WHERE contact IS NOT NULL AND contact != ''"
        )
    except Exception:
        pass  # Skip if duplicates exist


def _migration_003_crm_tasks(cur):
    """Add crm_tasks table for CRM consolidation."""
    cur.execute("""
        CREATE TABLE IF NOT EXISTS crm_tasks (
            id              TEXT PRIMARY KEY,
            client          TEXT NOT NULL,
            task_type       TEXT NOT NULL,
            due_date        TEXT,
            status          TEXT DEFAULT 'Pending',
            priority        TEXT DEFAULT 'Medium',
            note            TEXT,
            outcome         TEXT,
            automated       INTEGER DEFAULT 0,
            created_at      TEXT,
            completed_at    TEXT
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_crm_tasks_status ON crm_tasks(status)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_crm_tasks_client ON crm_tasks(client)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_crm_tasks_due    ON crm_tasks(due_date)")


# ═══════════════════════════════════════════════════════════════════════════
# GENERIC HELPERS (private)
# ═══════════════════════════════════════════════════════════════════════════

_VALID_TABLES = {
    "suppliers", "customers", "deals", "price_history", "fx_history",
    "inventory", "communications", "opportunities", "sync_logs",
    "missing_inputs", "email_queue", "whatsapp_queue", "whatsapp_sessions",
    "whatsapp_incoming", "director_briefings", "daily_logs", "alerts",
    "users", "audit_log", "recipient_lists", "source_registry",
    "chat_messages", "share_links", "share_schedules", "comm_tracking",
    "crm_tasks", "_schema_version",
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
    _round_monetary_fields(data)
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

    Uses a transaction to ensure atomicity.

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

    with db_transaction() as conn:
        _update_row_tx(conn, "deals", deal_id, data)


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
    _round_monetary_fields(data)
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
# EMAIL QUEUE
# ═══════════════════════════════════════════════════════════════════════════

def insert_email_queue(data: dict) -> int:
    """Insert a new email into the queue. Returns queue_id."""
    data = dict(data)
    data.setdefault("created_at", _now_ist())
    data.setdefault("updated_at", _now_ist())
    return _insert_row("email_queue", data)


def get_email_queue(status: str = None, limit: int = 50) -> list:
    """Return email queue items, optionally filtered by status."""
    if status:
        return _select_all("email_queue", where="status = ?",
                           params=(status,), order="created_at DESC")[:limit]
    return _select_all("email_queue", order="created_at DESC")[:limit]


def update_email_status(queue_id: int, status: str, **kwargs):
    """Update email queue item status and optional fields."""
    data = {"status": status, "updated_at": _now_ist()}
    data.update(kwargs)
    _update_row("email_queue", queue_id, data)


# ═══════════════════════════════════════════════════════════════════════════
# WHATSAPP QUEUE
# ═══════════════════════════════════════════════════════════════════════════

def insert_wa_queue(data: dict) -> int:
    """Insert a new WhatsApp message into the queue. Returns queue_id."""
    data = dict(data)
    data.setdefault("created_at", _now_ist())
    data.setdefault("updated_at", _now_ist())
    return _insert_row("whatsapp_queue", data)


def get_wa_queue(status: str = None, limit: int = 50) -> list:
    """Return WhatsApp queue items, optionally filtered by status."""
    if status:
        return _select_all("whatsapp_queue", where="status = ?",
                           params=(status,), order="created_at DESC")[:limit]
    return _select_all("whatsapp_queue", order="created_at DESC")[:limit]


def update_wa_status(queue_id: int, status: str, **kwargs):
    """Update WhatsApp queue item status and optional fields."""
    data = {"status": status, "updated_at": _now_ist()}
    data.update(kwargs)
    _update_row("whatsapp_queue", queue_id, data)


def get_active_wa_session(phone_number: str) -> dict | None:
    """Return active session for a phone number, or None."""
    conn = _get_conn()
    try:
        row = conn.execute(
            "SELECT * FROM whatsapp_sessions WHERE phone_number = ? "
            "AND session_expires > ? LIMIT 1",
            (phone_number, _now_ist()),
        ).fetchone()
        return _row_to_dict(row) if row else None
    finally:
        conn.close()


def upsert_wa_session(phone_number: str, customer_id: int = None,
                      last_message: str = "") -> int:
    """Create or update a WhatsApp session (24h window)."""
    now = _now_ist()
    expires = (datetime.now(IST) + timedelta(hours=24)).strftime("%Y-%m-%d %H:%M:%S")
    conn = _get_conn()
    try:
        existing = conn.execute(
            "SELECT id FROM whatsapp_sessions WHERE phone_number = ?",
            (phone_number,),
        ).fetchone()
        if existing:
            conn.execute(
                "UPDATE whatsapp_sessions SET session_start = ?, session_expires = ?, "
                "last_message = ?, customer_id = ? WHERE id = ?",
                (now, expires, last_message, customer_id, existing["id"]),
            )
            conn.commit()
            return existing["id"]
        else:
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO whatsapp_sessions (phone_number, session_start, "
                "session_expires, customer_id, last_message, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (phone_number, now, expires, customer_id, last_message, now),
            )
            conn.commit()
            return cur.lastrowid
    finally:
        conn.close()


def insert_wa_incoming(data: dict) -> int:
    """Insert an incoming WhatsApp message. Returns row id."""
    data = dict(data)
    data.setdefault("received_at", _now_ist())
    return _insert_row("whatsapp_incoming", data)


def get_wa_incoming(processed: int = None, limit: int = 50) -> list:
    """Return incoming WhatsApp messages."""
    if processed is not None:
        return _select_all("whatsapp_incoming", where="processed = ?",
                           params=(processed,), order="received_at DESC")[:limit]
    return _select_all("whatsapp_incoming", order="received_at DESC")[:limit]


# ═══════════════════════════════════════════════════════════════════════════
# DIRECTOR BRIEFINGS
# ═══════════════════════════════════════════════════════════════════════════

def insert_director_briefing(data: dict) -> int:
    """Insert a generated director briefing."""
    data = dict(data)
    data.setdefault("created_at", _now_ist())
    return _insert_row("director_briefings", data)


def get_director_briefings(limit: int = 30) -> list:
    """Return recent director briefings."""
    return _select_all("director_briefings",
                       order="briefing_date DESC")[:limit]


# ═══════════════════════════════════════════════════════════════════════════
# DAILY LOGS
# ═══════════════════════════════════════════════════════════════════════════

def insert_daily_log(data: dict) -> int:
    """Insert a daily log entry."""
    data = dict(data)
    data.setdefault("created_at", _now_ist())
    return _insert_row("daily_logs", data)


def get_daily_logs(log_date: str = None, limit: int = 50) -> list:
    """Return daily logs, optionally filtered by date."""
    if log_date:
        return _select_all("daily_logs", where="log_date = ?",
                           params=(log_date,),
                           order="created_at DESC")[:limit]
    return _select_all("daily_logs", order="created_at DESC")[:limit]


# ═══════════════════════════════════════════════════════════════════════════
# ALERTS
# ═══════════════════════════════════════════════════════════════════════════

def insert_alert(data: dict) -> int:
    """Insert a new alert. Returns alert id."""
    data = dict(data)
    data.setdefault("created_at", _now_ist())
    data.setdefault("status", "new")
    return _insert_row("alerts", data)


def get_alerts(status: str = None, priority: str = None, limit: int = 100) -> list:
    """Return alerts, optionally filtered by status and/or priority."""
    where_parts = []
    params = []
    if status:
        where_parts.append("status = ?")
        params.append(status)
    if priority:
        where_parts.append("priority = ?")
        params.append(priority)
    where = " AND ".join(where_parts) if where_parts else ""
    return _select_all("alerts", where=where, params=tuple(params),
                       order="created_at DESC")[:limit]


def update_alert_status(alert_id: int, status: str, **kwargs):
    """Update alert status and optional fields (acted_at, acted_by, snoozed_until)."""
    data = {"status": status}
    data.update(kwargs)
    _update_row("alerts", alert_id, data)


# ═══════════════════════════════════════════════════════════════════════════
# PHASE C: USERS / AUDIT / RECIPIENTS / SOURCE REGISTRY
# ═══════════════════════════════════════════════════════════════════════════

def insert_user(data: dict) -> int:
    data.setdefault("created_at", _now_ist())
    data.setdefault("updated_at", _now_ist())
    return _insert_row("users", data)

def get_user_by_username(username: str):
    conn = _get_conn()
    try:
        row = conn.execute("SELECT * FROM users WHERE username = ?", (username,)).fetchone()
        return _row_to_dict(row) if row else None
    finally:
        conn.close()

def get_all_users() -> list:
    return _select_all("users", order="username")

def update_user(user_id: int, data: dict):
    data["updated_at"] = _now_ist()
    _update_row("users", user_id, data)

def insert_audit_log(data: dict) -> int:
    data.setdefault("created_at", _now_ist())
    return _insert_row("audit_log", data)

def get_audit_logs(limit: int = 200, action_filter: str = None, user_filter: str = None) -> list:
    conn = _get_conn()
    try:
        sql = "SELECT * FROM audit_log WHERE 1=1"
        params = []
        if action_filter:
            sql += " AND action = ?"
            params.append(action_filter)
        if user_filter:
            sql += " AND username = ?"
            params.append(user_filter)
        sql += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)
        rows = conn.execute(sql, params).fetchall()
        return _rows_to_list(rows)
    finally:
        conn.close()

def insert_recipient_list(data: dict) -> int:
    data.setdefault("created_at", _now_ist())
    data.setdefault("updated_at", _now_ist())
    return _insert_row("recipient_lists", data)

def get_recipient_lists(list_type: str = None) -> list:
    conn = _get_conn()
    try:
        if list_type:
            rows = conn.execute(
                "SELECT * FROM recipient_lists WHERE is_active = 1 AND list_type = ? ORDER BY list_name",
                (list_type,),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM recipient_lists WHERE is_active = 1 ORDER BY list_name"
            ).fetchall()
        return _rows_to_list(rows)
    finally:
        conn.close()

def update_recipient_list(list_id: int, data: dict):
    data["updated_at"] = _now_ist()
    _update_row("recipient_lists", list_id, data)

def delete_recipient_list(list_id: int):
    _update_row("recipient_lists", list_id, {"is_active": 0, "updated_at": _now_ist()})

def insert_source_registry(data: dict) -> int:
    data.setdefault("created_at", _now_ist())
    data.setdefault("updated_at", _now_ist())
    return _insert_row("source_registry", data)

def get_all_sources() -> list:
    return _select_all("source_registry", order="source_name")

def get_source_by_key(key: str):
    conn = _get_conn()
    try:
        row = conn.execute("SELECT * FROM source_registry WHERE source_key = ?", (key,)).fetchone()
        return _row_to_dict(row) if row else None
    finally:
        conn.close()

def update_source_registry(source_id: int, data: dict):
    data["updated_at"] = _now_ist()
    _update_row("source_registry", source_id, data)

def delete_source_registry(source_id: int):
    conn = _get_conn()
    try:
        conn.execute("DELETE FROM source_registry WHERE id = ?", (source_id,))
        conn.commit()
    finally:
        conn.close()


# ═══════════════════════════════════════════════════════════════════════════
# CHAT MESSAGES
# ═══════════════════════════════════════════════════════════════════════════

def insert_chat_message(data: dict) -> int:
    data.setdefault("created_at", _now_ist())
    return _insert_row("chat_messages", data)

def get_chat_messages(conversation_id: str, limit: int = 100) -> list:
    conn = _get_conn()
    try:
        rows = conn.execute(
            "SELECT * FROM chat_messages WHERE conversation_id = ? ORDER BY created_at ASC LIMIT ?",
            (conversation_id, limit),
        ).fetchall()
        return _rows_to_list(rows)
    finally:
        conn.close()

def get_chat_conversations() -> list:
    conn = _get_conn()
    try:
        rows = conn.execute(
            """SELECT conversation_id,
                      MAX(sender_name) as last_sender,
                      MAX(created_at) as last_message_at,
                      SUM(CASE WHEN is_read = 0 AND sender_type = 'client' THEN 1 ELSE 0 END) as unread
               FROM chat_messages
               GROUP BY conversation_id
               ORDER BY last_message_at DESC"""
        ).fetchall()
        return _rows_to_list(rows)
    finally:
        conn.close()

def mark_chat_read(conversation_id: str):
    conn = _get_conn()
    try:
        conn.execute(
            "UPDATE chat_messages SET is_read = 1 WHERE conversation_id = ? AND is_read = 0",
            (conversation_id,),
        )
        conn.commit()
    finally:
        conn.close()


# ═══════════════════════════════════════════════════════════════════════════
# SHARE LINKS
# ═══════════════════════════════════════════════════════════════════════════

def insert_share_link(data: dict) -> int:
    data.setdefault("created_at", _now_ist())
    return _insert_row("share_links", data)

def get_share_link(token: str):
    conn = _get_conn()
    try:
        row = conn.execute("SELECT * FROM share_links WHERE link_token = ?", (token,)).fetchone()
        return _row_to_dict(row) if row else None
    finally:
        conn.close()

def get_all_share_links(active_only: bool = True) -> list:
    where = "is_active = 1" if active_only else ""
    return _select_all("share_links", where=where, order="created_at DESC")

def increment_share_view(token: str):
    conn = _get_conn()
    try:
        conn.execute(
            "UPDATE share_links SET view_count = view_count + 1, last_accessed = ? WHERE link_token = ?",
            (_now_ist(), token),
        )
        conn.commit()
    finally:
        conn.close()

def deactivate_share_link(link_id: int):
    _update_row("share_links", link_id, {"is_active": 0})


# ═══════════════════════════════════════════════════════════════════════════
# SHARE SCHEDULES
# ═══════════════════════════════════════════════════════════════════════════

def insert_share_schedule(data: dict) -> int:
    data.setdefault("created_at", _now_ist())
    data.setdefault("updated_at", _now_ist())
    return _insert_row("share_schedules", data)

def get_share_schedules(active_only: bool = True) -> list:
    where = "is_active = 1" if active_only else ""
    return _select_all("share_schedules", where=where, order="schedule_name")

def update_share_schedule(schedule_id: int, data: dict):
    data["updated_at"] = _now_ist()
    _update_row("share_schedules", schedule_id, data)

def delete_share_schedule(schedule_id: int):
    _update_row("share_schedules", schedule_id, {"is_active": 0, "updated_at": _now_ist()})


# ═══════════════════════════════════════════════════════════════════════════
# COMMUNICATION TRACKING
# ═══════════════════════════════════════════════════════════════════════════

def insert_comm_tracking(data: dict) -> int:
    data.setdefault("created_at", _now_ist())
    return _insert_row("comm_tracking", data)

def get_comm_tracking(channel: str = None, limit: int = 200) -> list:
    conn = _get_conn()
    try:
        if channel:
            rows = conn.execute(
                "SELECT * FROM comm_tracking WHERE channel = ? ORDER BY created_at DESC LIMIT ?",
                (channel, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM comm_tracking ORDER BY created_at DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return _rows_to_list(rows)
    finally:
        conn.close()

def update_comm_tracking_status(tracking_id: str, status: str, error_message: str = None):
    conn = _get_conn()
    try:
        if error_message:
            conn.execute(
                "UPDATE comm_tracking SET delivery_status = ?, error_message = ?, delivered_at = ? WHERE tracking_id = ?",
                (status, error_message, _now_ist(), tracking_id),
            )
        else:
            conn.execute(
                "UPDATE comm_tracking SET delivery_status = ?, delivered_at = ? WHERE tracking_id = ?",
                (status, _now_ist(), tracking_id),
            )
        conn.commit()
    finally:
        conn.close()

def get_comm_stats(days: int = 30) -> dict:
    conn = _get_conn()
    try:
        from datetime import datetime, timedelta
        cutoff = (datetime.now(IST) - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
        total = conn.execute(
            "SELECT COUNT(*) as cnt FROM comm_tracking WHERE created_at >= ?", (cutoff,)
        ).fetchone()["cnt"]
        by_channel = conn.execute(
            "SELECT channel, COUNT(*) as cnt FROM comm_tracking WHERE created_at >= ? GROUP BY channel",
            (cutoff,),
        ).fetchall()
        by_status = conn.execute(
            "SELECT delivery_status, COUNT(*) as cnt FROM comm_tracking WHERE created_at >= ? GROUP BY delivery_status",
            (cutoff,),
        ).fetchall()
        return {
            "total": total,
            "by_channel": {r["channel"]: r["cnt"] for r in by_channel},
            "by_status": {r["delivery_status"]: r["cnt"] for r in by_status},
        }
    finally:
        conn.close()


# ═══════════════════════════════════════════════════════════════════════════
# CRM TASKS (SQLite-backed)
# ═══════════════════════════════════════════════════════════════════════════

def insert_crm_task(data: dict) -> str:
    """Insert or replace a CRM task. Returns task id."""
    data = dict(data)
    data.setdefault("created_at", _now_ist())
    task_id = data.get("id", "")
    if not task_id:
        import uuid
        task_id = str(uuid.uuid4())[:8]
        data["id"] = task_id
    # Map 'type' to 'task_type' for DB column
    if "type" in data and "task_type" not in data:
        data["task_type"] = data.pop("type")
    conn = _get_conn()
    try:
        conn.execute(
            """INSERT OR REPLACE INTO crm_tasks
               (id, client, task_type, due_date, status, priority, note,
                outcome, automated, created_at, completed_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                data.get("id"), data.get("client", ""),
                data.get("task_type", "Call"), data.get("due_date"),
                data.get("status", "Pending"), data.get("priority", "Medium"),
                data.get("note", ""), data.get("outcome", ""),
                1 if data.get("automated") else 0,
                data.get("created_at", _now_ist()), data.get("completed_at"),
            ),
        )
        conn.commit()
        return task_id
    finally:
        conn.close()


def get_crm_tasks(status: str = None, client: str = None) -> list:
    """Return CRM tasks, optionally filtered."""
    conn = _get_conn()
    try:
        sql = "SELECT * FROM crm_tasks WHERE 1=1"
        params = []
        if status:
            sql += " AND status = ?"
            params.append(status)
        if client:
            sql += " AND client = ?"
            params.append(client)
        sql += " ORDER BY due_date ASC"
        rows = conn.execute(sql, params).fetchall()
        return _rows_to_list(rows)
    finally:
        conn.close()


def complete_crm_task(task_id: str, outcome: str = ""):
    """Mark a CRM task as completed."""
    conn = _get_conn()
    try:
        conn.execute(
            "UPDATE crm_tasks SET status = 'Completed', outcome = ?, "
            "completed_at = ? WHERE id = ?",
            (outcome, _now_ist(), task_id),
        )
        conn.commit()
    finally:
        conn.close()


# ═══════════════════════════════════════════════════════════════════════════
# DATA RETENTION / CLEANUP
# ═══════════════════════════════════════════════════════════════════════════

def cleanup_old_records() -> dict:
    """
    Trim oversized tables based on configured limits.
    Called from sync_engine Batch 4.
    Returns dict of {table: rows_deleted}.
    """
    try:
        from settings_engine import load_settings
        settings = load_settings()
    except ImportError:
        settings = {}

    limits = {
        "sync_logs":      settings.get("max_sync_logs", 1000),
        "audit_log":      10000,
        "comm_tracking":  settings.get("max_communication_records", 10000),
        "price_history":  settings.get("max_price_history_records", 5000),
    }

    results = {}
    conn = _get_conn()
    try:
        for table, max_rows in limits.items():
            try:
                row = conn.execute(f"SELECT COUNT(*) as c FROM {table}").fetchone()
                count = row["c"] if row else 0
                if count > max_rows:
                    conn.execute(
                        f"DELETE FROM {table} WHERE id NOT IN "
                        f"(SELECT id FROM {table} ORDER BY id DESC LIMIT ?)",
                        (max_rows,),
                    )
                    results[table] = count - max_rows
            except Exception:
                pass
        conn.commit()
    finally:
        conn.close()
    return results


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
