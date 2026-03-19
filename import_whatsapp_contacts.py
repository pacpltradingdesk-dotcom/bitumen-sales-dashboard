"""
Import WhatsApp Contacts from Bitumen_Customer_Profile_WhatsApp.xlsx
into Dashboard's SQLite DB + JSON files.

Destinations:
1. contacts table (ALL 25,628)
2. customers table (buyers, contractors, dealers, enterprises)
3. suppliers table (manufacturers, importers, exporters, refineries)
4. sales_parties.json (customer JSON backup)
5. purchase_parties.json (supplier JSON - enrich existing)
6. service_providers.json (transporters)
"""

import pandas as pd
import sqlite3
import json
import os
import re
from datetime import datetime

# -- Config --------------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
EXCEL_PATH = os.path.join(os.path.dirname(BASE_DIR), "Bitumen_Customer_Profile_WhatsApp.xlsx")
DB_PATH = os.path.join(BASE_DIR, "bitumen_dashboard.db")
NOW_IST = datetime.now().strftime("%d-%m-%Y %H:%M:%S IST")
NOW_ISO = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# -- Category Mapping ---------------------------------------------------------
CUSTOMER_CATEGORIES = {
    "Contractor", "Dealer", "Buyer", "Bitumen Buyer", "Enterprise",
    "Emulsion Contractor", "Construction"
}

SUPPLIER_CATEGORIES = {
    "Manufacturer", "Importer", "International", "Exporter",
    "Supplier", "Private Refinery Bitumen", "Emulsion Importer"
}

TRADER_CATEGORIES = {
    "Trader", "Bitumen Trader", "Emulsion Trader", "Agent/Broker"
}

TRANSPORTER_CATEGORIES = {"Transporter"}

# -- Helpers -------------------------------------------------------------------
def clean_phone(phone):
    """Normalize Indian phone number to 10-digit format."""
    if pd.isna(phone):
        return ""
    phone = str(phone).strip()
    # Handle scientific notation from Excel (9.598009e+09)
    try:
        if 'e' in phone.lower() or '.' in phone:
            phone = str(int(float(phone)))
    except (ValueError, OverflowError):
        pass
    # Strip non-digits
    digits = re.sub(r'[^\d]', '', phone)
    # Remove leading 91 or 0
    if len(digits) == 12 and digits.startswith('91'):
        digits = digits[2:]
    elif len(digits) == 11 and digits.startswith('0'):
        digits = digits[1:]
    # Validate 10-digit Indian mobile
    if len(digits) == 10 and digits[0] in '6789':
        return digits
    # Return raw if international or unusual
    if len(digits) >= 7:
        return digits
    return ""


def safe_str(val):
    """Convert to string, return empty string for NaN/None."""
    if pd.isna(val):
        return ""
    return str(val).strip()


def map_buyer_seller_tag(category):
    if category in CUSTOMER_CATEGORIES:
        return "buyer"
    elif category in SUPPLIER_CATEGORIES:
        return "seller"
    elif category in TRADER_CATEGORIES:
        return "both"
    elif category in TRANSPORTER_CATEGORIES:
        return "service_provider"
    else:
        return "unknown"


def map_contact_type(category):
    if category in CUSTOMER_CATEGORIES:
        return "customer"
    elif category in SUPPLIER_CATEGORIES:
        return "supplier"
    elif category in TRADER_CATEGORIES:
        return "prospect"
    elif category in TRANSPORTER_CATEGORIES:
        return "service_provider"
    else:
        return "prospect"


# -- Main Import ---------------------------------------------------------------
def main():
    print("=" * 70)
    print("  WHATSAPP CONTACTS IMPORT -> BITUMEN DASHBOARD")
    print("=" * 70)

    # 1. Read Excel
    print(f"\n[1/7] Reading Excel: {EXCEL_PATH}")
    df = pd.read_excel(EXCEL_PATH, sheet_name="All Bitumen Contacts")
    print(f"  -> {len(df)} contacts loaded, {len(df.columns)} columns")

    # 2. Clean data
    print("\n[2/7] Cleaning & normalizing data...")
    df['phone_clean'] = df['WhatsApp Phone'].apply(clean_phone)
    df['phone2_clean'] = df['Phone 2'].apply(clean_phone)
    df['Name'] = df['Name'].apply(safe_str)
    df['Company'] = df['Company'].apply(safe_str)
    df['Category'] = df['Category'].apply(safe_str)
    df['Email'] = df['Email'].apply(safe_str)
    df['City'] = df['City'].apply(safe_str)
    df['State'] = df['State'].apply(safe_str)
    df['Pincode'] = df['Pincode'].apply(lambda x: safe_str(x).replace('.0', ''))
    df['Full Address'] = df['Full Address'].apply(safe_str)
    df['GST Number'] = df['GST Number'].apply(safe_str)
    df['PAN'] = df['PAN'].apply(safe_str)
    df['Remark'] = df['Remark'].apply(safe_str)
    df['Products'] = df['Products'].apply(safe_str)
    df['District'] = df['District'].apply(safe_str)

    # Deduplicate by phone (keep first occurrence)
    before = len(df)
    df = df.drop_duplicates(subset='phone_clean', keep='first')
    df = df[df['phone_clean'] != ""]
    after = len(df)
    print(f"  -> Deduped: {before} -> {after} unique phone contacts ({before - after} removed)")

    # 3. Connect DB
    print("\n[3/7] Connecting to SQLite database...")
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    cursor = conn.cursor()

    # Get existing phone numbers to avoid duplicates
    existing_contacts = set()
    try:
        cursor.execute("SELECT mobile1 FROM contacts WHERE mobile1 IS NOT NULL AND mobile1 != ''")
        existing_contacts = {row[0] for row in cursor.fetchall()}
    except:
        pass

    existing_customers = set()
    try:
        cursor.execute("SELECT contact FROM customers WHERE contact IS NOT NULL AND contact != ''")
        existing_customers = {row[0] for row in cursor.fetchall()}
        cursor.execute("SELECT whatsapp_number FROM customers WHERE whatsapp_number IS NOT NULL AND whatsapp_number != ''")
        existing_customers.update({row[0] for row in cursor.fetchall()})
    except:
        pass

    existing_suppliers_phones = set()
    try:
        cursor.execute("SELECT contact FROM suppliers WHERE contact IS NOT NULL AND contact != ''")
        existing_suppliers_phones = {row[0] for row in cursor.fetchall()}
        cursor.execute("SELECT whatsapp_number FROM suppliers WHERE whatsapp_number IS NOT NULL AND whatsapp_number != ''")
        existing_suppliers_phones.update({row[0] for row in cursor.fetchall()})
    except:
        pass

    print(f"  -> Existing: {len(existing_contacts)} contacts, {len(existing_customers)} customers, {len(existing_suppliers_phones)} suppliers")

    # 4. Insert into contacts table (ALL)
    print("\n[4/7] Inserting into CONTACTS table (master)...")
    contacts_added = 0
    contacts_skipped = 0

    for _, row in df.iterrows():
        phone = row['phone_clean']
        if phone in existing_contacts:
            contacts_skipped += 1
            continue

        display_name = row['Company'] if row['Company'] else row['Name']
        category = row['Category']

        cursor.execute("""
            INSERT INTO contacts (
                name, company_name, contact_type, category, buyer_seller_tag,
                city, state, mobile1, mobile2, email, gstin, pan,
                address, pincode, products_dealt, preferred_language,
                whatsapp_opted_in, email_opted_in, source, is_active,
                notes, created_at, updated_at, vip_score, vip_tier
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'en',
                      1, ?, 'whatsapp_import', 1, ?, ?, ?, 0.0, 'standard')
        """, (
            display_name,
            row['Company'],
            map_contact_type(category),
            category,
            map_buyer_seller_tag(category),
            row['City'],
            row['State'],
            phone,
            row['phone2_clean'],
            row['Email'],
            row['GST Number'],
            row['PAN'],
            row['Full Address'],
            row['Pincode'],
            row['Products'],
            1 if row['Email'] else 0,
            row['Remark'],
            NOW_ISO,
            NOW_ISO,
        ))
        existing_contacts.add(phone)
        contacts_added += 1

    conn.commit()
    print(f"  -> Added: {contacts_added}, Skipped (duplicate): {contacts_skipped}")

    # 5. Insert into customers table
    print("\n[5/7] Inserting into CUSTOMERS table...")
    customer_df = df[df['Category'].isin(CUSTOMER_CATEGORIES)]
    cust_added = 0
    cust_skipped = 0

    for _, row in customer_df.iterrows():
        phone = row['phone_clean']
        if phone in existing_customers:
            cust_skipped += 1
            continue

        display_name = row['Company'] if row['Company'] else row['Name']

        cursor.execute("""
            INSERT INTO customers (
                name, category, city, state, contact, gstin, address,
                preferred_grades, relationship_stage, notes, is_active,
                created_at, updated_at, email, whatsapp_number
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'cold', ?, 1, ?, ?, ?, ?)
        """, (
            display_name,
            row['Category'],
            row['City'],
            row['State'],
            phone,
            row['GST Number'],
            row['Full Address'],
            row['Products'],
            row['Remark'],
            NOW_IST,
            NOW_IST,
            row['Email'],
            phone,
        ))
        existing_customers.add(phone)
        cust_added += 1

    conn.commit()
    print(f"  -> Added: {cust_added}, Skipped (duplicate): {cust_skipped}")

    # 6. Insert into suppliers table
    print("\n[6/7] Inserting into SUPPLIERS table...")
    supplier_df = df[df['Category'].isin(SUPPLIER_CATEGORIES)]
    supp_added = 0
    supp_skipped = 0

    for _, row in supplier_df.iterrows():
        phone = row['phone_clean']
        if phone in existing_suppliers_phones:
            supp_skipped += 1
            continue

        display_name = row['Company'] if row['Company'] else row['Name']

        cursor.execute("""
            INSERT INTO suppliers (
                name, category, city, state, contact, gstin, pan,
                preferred_grades, relationship_stage, notes, is_active,
                marked_for_purchase, created_at, updated_at, email, whatsapp_number
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'cold', ?, 1, 0, ?, ?, ?, ?)
        """, (
            display_name,
            row['Category'],
            row['City'],
            row['State'],
            phone,
            row['GST Number'],
            row['PAN'],
            row['Products'],
            row['Remark'],
            NOW_IST,
            NOW_IST,
            row['Email'],
            phone,
        ))
        existing_suppliers_phones.add(phone)
        supp_added += 1

    conn.commit()
    print(f"  -> Added: {supp_added}, Skipped (duplicate): {supp_skipped}")

    # 7. Update JSON files for backward compatibility
    print("\n[7/7] Updating JSON files...")

    # --- sales_parties.json ---
    sales_parties = []
    cursor.execute("SELECT name, category, city, state, contact, gstin, address, is_active FROM customers")
    for r in cursor.fetchall():
        sales_parties.append({
            "name": r[0] or "",
            "category": r[1] or "",
            "city": r[2] or "",
            "state": r[3] or "",
            "contact": r[4] or "",
            "gstin": r[5] or "",
            "address": r[6] or "",
            "active": bool(r[7])
        })

    with open(os.path.join(BASE_DIR, "sales_parties.json"), "w", encoding="utf-8") as f:
        json.dump(sales_parties, f, indent=2, ensure_ascii=False)
    print(f"  -> sales_parties.json: {len(sales_parties)} entries")

    # --- purchase_parties.json ---
    purchase_parties = []
    cursor.execute("SELECT name, category, city, state, contact, gstin, is_active, marked_for_purchase FROM suppliers")
    for r in cursor.fetchall():
        purchase_parties.append({
            "name": r[0] or "",
            "type": r[1] or "",
            "city": r[2] or "",
            "state": r[3] or "",
            "contact": r[4] or "",
            "gstin": r[5] or "",
            "marked_for_purchase": bool(r[7])
        })

    with open(os.path.join(BASE_DIR, "purchase_parties.json"), "w", encoding="utf-8") as f:
        json.dump(purchase_parties, f, indent=2, ensure_ascii=False)
    print(f"  -> purchase_parties.json: {len(purchase_parties)} entries")

    # --- service_providers.json (add transporters) ---
    existing_sp = []
    sp_path = os.path.join(BASE_DIR, "service_providers.json")
    try:
        with open(sp_path, "r", encoding="utf-8") as f:
            existing_sp = json.load(f)
    except:
        pass

    existing_sp_names = {sp.get("name", "").lower() for sp in existing_sp}
    transporter_df = df[df['Category'].isin(TRANSPORTER_CATEGORIES)]
    tp_added = 0

    for _, row in transporter_df.iterrows():
        display_name = row['Company'] if row['Company'] else row['Name']
        if display_name.lower() in existing_sp_names:
            continue
        existing_sp.append({
            "name": display_name,
            "category": "Transporter - Bulk",
            "city": row['City'] if row['City'] else "",
            "state": row['State'] if row['State'] else "",
            "contact": row['phone_clean'],
            "details": f"WhatsApp Import. {row['Remark']}" if row['Remark'] else "WhatsApp Import."
        })
        existing_sp_names.add(display_name.lower())
        tp_added += 1

    with open(sp_path, "w", encoding="utf-8") as f:
        json.dump(existing_sp, f, indent=2, ensure_ascii=False)
    print(f"  -> service_providers.json: {len(existing_sp)} entries ({tp_added} transporters added)")

    # -- Final Stats -----------------------------------------------------------
    cursor.execute("SELECT COUNT(*) FROM contacts")
    total_contacts = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM customers")
    total_customers = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM suppliers")
    total_suppliers = cursor.fetchone()[0]

    conn.close()

    print("\n" + "=" * 70)
    print("  IMPORT COMPLETE!")
    print("=" * 70)
    print(f"""
  Database Stats (after import):
  +------------------------+-----------┐
  | contacts (master)      | {total_contacts:>9,} |
  | customers (buyers)     | {total_customers:>9,} |
  | suppliers (sellers)    | {total_suppliers:>9,} |
  | sales_parties.json     | {len(sales_parties):>9,} |
  | purchase_parties.json  | {len(purchase_parties):>9,} |
  | service_providers.json | {len(existing_sp):>9,} |
  +------------------------+-----------+
    """)


if __name__ == "__main__":
    main()
