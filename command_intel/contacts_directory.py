"""
Contacts Directory — View all imported contacts (Suppliers, Customers, All)
with search, filter, and export capabilities.
"""
import streamlit as st
import sqlite3
import pandas as pd
import os

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "bitumen_dashboard.db")


def _get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _fmt_phone(phone):
    """Format 10-digit phone as +91 XXXXX XXXXX."""
    if not phone:
        return ""
    phone = str(phone).strip()
    if len(phone) == 10:
        return f"+91 {phone[:5]} {phone[5:]}"
    return phone


def render_contacts_directory():
    """Main render function for Contacts Directory page."""

    # ── Determine initial tab from session state ──────────────────────────────
    filter_mode = st.session_state.pop("_contacts_filter", "all")
    tab_index = {"suppliers": 1, "customers": 2, "all": 0}.get(filter_mode, 0)

    # ── Header ────────────────────────────────────────────────────────────────
    st.markdown("""
    <div style="background:linear-gradient(135deg,#1e3a5f 0%,#2d6a4f 100%);
                padding:18px 24px;border-radius:10px;margin-bottom:16px;">
      <span style="color:#fff;font-size:1.3rem;font-weight:800;">
        Contacts Directory
      </span>
      <span style="color:#86efac;font-size:0.85rem;margin-left:12px;">
        WhatsApp Import Database
      </span>
    </div>
    """, unsafe_allow_html=True)

    # ── Stats bar ─────────────────────────────────────────────────────────────
    conn = _get_conn()
    cursor = conn.cursor()

    total_contacts = cursor.execute("SELECT COUNT(*) FROM contacts").fetchone()[0]
    total_customers = cursor.execute("SELECT COUNT(*) FROM customers").fetchone()[0]
    total_suppliers = cursor.execute("SELECT COUNT(*) FROM suppliers").fetchone()[0]

    s1, s2, s3, s4 = st.columns(4)
    s1.metric("All Contacts", f"{total_contacts:,}")
    s2.metric("Suppliers", f"{total_suppliers:,}")
    s3.metric("Customers", f"{total_customers:,}")
    s4.metric("With GST", cursor.execute(
        "SELECT COUNT(*) FROM contacts WHERE gstin IS NOT NULL AND gstin != ''").fetchone()[0])

    # ── Tabs ──────────────────────────────────────────────────────────────────
    tab_all, tab_supp, tab_cust = st.tabs([
        f"All Contacts ({total_contacts:,})",
        f"Suppliers / Exporters ({total_suppliers:,})",
        f"Customers / Buyers ({total_customers:,})"
    ])

    # ── Helper: render a searchable table ─────────────────────────────────────
    def _render_table(query, table_name, columns_config):
        """Generic searchable data table renderer."""
        df = pd.read_sql_query(query, conn)

        if df.empty:
            st.info("No records found.")
            return

        # Search bar
        col_search, col_state, col_cat = st.columns([2, 1, 1])
        with col_search:
            search = st.text_input(
                "Search by name, phone, city, GST...",
                key=f"search_{table_name}",
                placeholder="Type to search..."
            )
        with col_state:
            states = ["All States"] + sorted(df['state'].dropna().unique().tolist()) if 'state' in df.columns else ["All States"]
            sel_state = st.selectbox("State", states, key=f"state_{table_name}")
        with col_cat:
            cats = ["All Categories"] + sorted(df['category'].dropna().unique().tolist()) if 'category' in df.columns else ["All Categories"]
            sel_cat = st.selectbox("Category", cats, key=f"cat_{table_name}")

        # Apply filters
        filtered = df.copy()
        if search:
            search_lower = search.lower()
            mask = filtered.apply(
                lambda row: any(search_lower in str(v).lower() for v in row.values), axis=1
            )
            filtered = filtered[mask]
        if sel_state != "All States" and 'state' in filtered.columns:
            filtered = filtered[filtered['state'] == sel_state]
        if sel_cat != "All Categories" and 'category' in filtered.columns:
            filtered = filtered[filtered['category'] == sel_cat]

        # Results count
        st.markdown(f"""
        <div style="font-size:0.78rem;color:#475569;margin-bottom:8px;">
            Showing <b>{len(filtered):,}</b> of {len(df):,} records
        </div>
        """, unsafe_allow_html=True)

        # Display dataframe
        st.dataframe(
            filtered,
            use_container_width=True,
            height=500,
            column_config=columns_config,
            hide_index=True,
        )

        # Export button
        csv = filtered.to_csv(index=False).encode('utf-8')
        st.download_button(
            f"Download {table_name} CSV ({len(filtered):,} rows)",
            csv,
            f"{table_name}_{len(filtered)}_records.csv",
            "text/csv",
            key=f"dl_{table_name}",
            use_container_width=True,
        )

    # ── TAB 1: All Contacts ───────────────────────────────────────────────────
    with tab_all:
        _render_table(
            """SELECT name, company_name as company, category, mobile1 as phone,
                      mobile2 as phone_2, email, city, state, gstin as gst,
                      pan, buyer_seller_tag as type, source
               FROM contacts ORDER BY state, city, name""",
            "all_contacts",
            {
                "name": st.column_config.TextColumn("Name", width="medium"),
                "company": st.column_config.TextColumn("Company", width="medium"),
                "category": st.column_config.TextColumn("Category", width="small"),
                "phone": st.column_config.TextColumn("Phone", width="small"),
                "phone_2": st.column_config.TextColumn("Phone 2", width="small"),
                "email": st.column_config.TextColumn("Email", width="medium"),
                "city": st.column_config.TextColumn("City", width="small"),
                "state": st.column_config.TextColumn("State", width="small"),
                "gst": st.column_config.TextColumn("GST", width="medium"),
                "pan": st.column_config.TextColumn("PAN", width="small"),
                "type": st.column_config.TextColumn("Type", width="small"),
                "source": st.column_config.TextColumn("Source", width="small"),
            }
        )

    # ── TAB 2: Suppliers / Exporters ──────────────────────────────────────────
    with tab_supp:
        _render_table(
            """SELECT name, category, city, state, contact as phone,
                      email, whatsapp_number as whatsapp, gstin as gst, pan,
                      preferred_grades as grades, relationship_stage as stage,
                      notes
               FROM suppliers ORDER BY state, city, name""",
            "suppliers",
            {
                "name": st.column_config.TextColumn("Name", width="medium"),
                "category": st.column_config.TextColumn("Category", width="small"),
                "city": st.column_config.TextColumn("City", width="small"),
                "state": st.column_config.TextColumn("State", width="small"),
                "phone": st.column_config.TextColumn("Phone", width="small"),
                "email": st.column_config.TextColumn("Email", width="small"),
                "whatsapp": st.column_config.TextColumn("WhatsApp", width="small"),
                "gst": st.column_config.TextColumn("GST", width="medium"),
                "pan": st.column_config.TextColumn("PAN", width="small"),
                "grades": st.column_config.TextColumn("Grades", width="medium"),
                "stage": st.column_config.TextColumn("Stage", width="small"),
                "notes": st.column_config.TextColumn("Notes", width="medium"),
            }
        )

    # ── TAB 3: Customers / Buyers ─────────────────────────────────────────────
    with tab_cust:
        _render_table(
            """SELECT name, category, city, state, contact as phone,
                      email, whatsapp_number as whatsapp, gstin as gst,
                      preferred_grades as grades, relationship_stage as stage,
                      address, notes
               FROM customers ORDER BY state, city, name""",
            "customers",
            {
                "name": st.column_config.TextColumn("Name", width="medium"),
                "category": st.column_config.TextColumn("Category", width="small"),
                "city": st.column_config.TextColumn("City", width="small"),
                "state": st.column_config.TextColumn("State", width="small"),
                "phone": st.column_config.TextColumn("Phone", width="small"),
                "email": st.column_config.TextColumn("Email", width="small"),
                "whatsapp": st.column_config.TextColumn("WhatsApp", width="small"),
                "gst": st.column_config.TextColumn("GST", width="medium"),
                "grades": st.column_config.TextColumn("Grades", width="medium"),
                "stage": st.column_config.TextColumn("Stage", width="small"),
                "address": st.column_config.TextColumn("Address", width="medium"),
                "notes": st.column_config.TextColumn("Notes", width="medium"),
            }
        )

    # ── Category breakdown chart ──────────────────────────────────────────────
    st.markdown("---")
    st.markdown("### Category Breakdown")
    cat_df = pd.read_sql_query(
        "SELECT category, COUNT(*) as count FROM contacts GROUP BY category ORDER BY count DESC",
        conn
    )
    if not cat_df.empty:
        c1, c2 = st.columns(2)
        with c1:
            st.bar_chart(cat_df.set_index('category')['count'], height=400)
        with c2:
            state_df = pd.read_sql_query(
                """SELECT state, COUNT(*) as count FROM contacts
                   WHERE state IS NOT NULL AND state != ''
                   GROUP BY state ORDER BY count DESC LIMIT 20""",
                conn
            )
            st.markdown("### Top 20 States")
            st.bar_chart(state_df.set_index('state')['count'], height=400)

    conn.close()
