"""
Contacts Directory — View all imported contacts (Suppliers, Customers, All)
with search, filter, and export capabilities.
Default sort: GST/PAN filled records first.
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


def render_contacts_directory():
    """Main render function for Contacts Directory page."""

    # ── Determine initial tab from session state ──────────────────────────────
    filter_mode = st.session_state.pop("_contacts_filter", "all")

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
    with_gst = cursor.execute(
        "SELECT COUNT(*) FROM contacts WHERE gstin IS NOT NULL AND gstin != ''").fetchone()[0]
    with_pan = cursor.execute(
        "SELECT COUNT(*) FROM contacts WHERE pan IS NOT NULL AND pan != ''").fetchone()[0]

    s1, s2, s3, s4, s5 = st.columns(5)
    s1.metric("All Contacts", f"{total_contacts:,}")
    s2.metric("Suppliers", f"{total_suppliers:,}")
    s3.metric("Customers", f"{total_customers:,}")
    s4.metric("With GST", f"{with_gst:,}")
    s5.metric("With PAN", f"{with_pan:,}")

    # ── Tabs ──────────────────────────────────────────────────────────────────
    tab_all, tab_supp, tab_cust = st.tabs([
        f"All Contacts ({total_contacts:,})",
        f"Suppliers / Exporters ({total_suppliers:,})",
        f"Customers / Buyers ({total_customers:,})"
    ])

    # ── Helper: render a searchable table ─────────────────────────────────────
    def _render_table(query, table_name, columns_config, gst_col="gst", pan_col="pan"):
        """Generic searchable data table renderer."""
        df = pd.read_sql_query(query, conn)

        if df.empty:
            st.info("No records found.")
            return

        # Replace None/NaN with empty string for display
        df = df.fillna("")
        df = df.replace("None", "")

        # Filters row
        col_search, col_state, col_cat, col_gst = st.columns([2, 1, 1, 1])
        with col_search:
            search = st.text_input(
                "Search by name, phone, city, GST...",
                key=f"search_{table_name}",
                placeholder="Type to search..."
            )
        with col_state:
            states_list = sorted([s for s in df['state'].unique().tolist() if s]) if 'state' in df.columns else []
            states = ["All States"] + states_list
            sel_state = st.selectbox("State", states, key=f"state_{table_name}")
        with col_cat:
            cats_list = sorted([c for c in df['category'].unique().tolist() if c]) if 'category' in df.columns else []
            cats = ["All Categories"] + cats_list
            sel_cat = st.selectbox("Category", cats, key=f"cat_{table_name}")
        with col_gst:
            gst_filter = st.selectbox("Data Filter", [
                "GST+PAN filled first",
                "Only with GST",
                "Only with PAN",
                "Only with GST & PAN",
                "Show All"
            ], key=f"gst_{table_name}")

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

        # GST/PAN filter & sorting
        if gst_col in filtered.columns and pan_col in filtered.columns:
            if gst_filter == "Only with GST":
                filtered = filtered[filtered[gst_col] != ""]
            elif gst_filter == "Only with PAN":
                filtered = filtered[filtered[pan_col] != ""]
            elif gst_filter == "Only with GST & PAN":
                filtered = filtered[(filtered[gst_col] != "") & (filtered[pan_col] != "")]
            # Default sort: GST+PAN filled first
            if gst_filter in ("GST+PAN filled first", "Show All"):
                filtered['_has_gst'] = (filtered[gst_col] != "").astype(int)
                filtered['_has_pan'] = (filtered[pan_col] != "").astype(int)
                filtered['_sort_score'] = filtered['_has_gst'] + filtered['_has_pan']
                filtered = filtered.sort_values('_sort_score', ascending=False)
                filtered = filtered.drop(columns=['_has_gst', '_has_pan', '_sort_score'])
        elif gst_col in filtered.columns:
            if gst_filter == "Only with GST":
                filtered = filtered[filtered[gst_col] != ""]
            if gst_filter in ("GST+PAN filled first", "Show All"):
                filtered['_has_gst'] = (filtered[gst_col] != "").astype(int)
                filtered = filtered.sort_values('_has_gst', ascending=False)
                filtered = filtered.drop(columns=['_has_gst'])

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
               FROM contacts
               ORDER BY (CASE WHEN gstin IS NOT NULL AND gstin != '' THEN 0 ELSE 1 END),
                        (CASE WHEN pan IS NOT NULL AND pan != '' THEN 0 ELSE 1 END),
                        state, city, name""",
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
               FROM suppliers
               ORDER BY (CASE WHEN gstin IS NOT NULL AND gstin != '' THEN 0 ELSE 1 END),
                        (CASE WHEN pan IS NOT NULL AND pan != '' THEN 0 ELSE 1 END),
                        state, city, name""",
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
               FROM customers
               ORDER BY (CASE WHEN gstin IS NOT NULL AND gstin != '' THEN 0 ELSE 1 END),
                        state, city, name""",
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
            },
            pan_col="_no_pan_col"  # customers table has no pan column
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
