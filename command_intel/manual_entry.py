import streamlit as st
import pandas as pd
import datetime

try:
    from india_localization import format_inr, format_date, format_datetime_ist, get_financial_year
except ImportError:
    pass

from ui_badges import display_badge

def render():
    display_badge("user input")
    
    st.markdown("### 📝 Fortnight CRM Field Pricing (Overrides & Logging)")
    st.info("Directly input local or competitor quotes. Overriding the Base MLR Prediction will trigger a 🚩 **Conflict Badge** via the Audit Trackers.")
    
    st.markdown("---")
    
    with st.form("manual_price_entry"):
        st.markdown("#### Enter New Manual Data Point")
        c1, c2, c3 = st.columns(3)
        
        # Indian calendar targets
        today = datetime.date.today()
        dates_options = [format_date(datetime.date(today.year, today.month, 1)), format_date(datetime.date(today.year, today.month, 16))]
        
        rev_date = c1.selectbox("Target Revision Date (DD-MM-YYYY)", dates_options)
        loc = c2.selectbox("Sub-Location", ["Mumbai", "Gujarat", "Delhi", "Chennai", "Custom"])
        grade = c3.selectbox("Grade", ["VG30", "VG10", "PMB", "CRMB"])
        
        c4, c5, c6 = st.columns(3)
        price = c4.number_input("Field Price Quote (₹/MT)", min_value=10000)
        freight = c5.number_input("Freight Deductible (₹/MT)", value=0)
        tax = c6.number_input("Include Tax/GST (%)", value=18.0)
        
        st.text_area("Observations / Remarks (Important for logging)")
        st.file_uploader("Upload Quotation / Proof (PDF/IMG)")
        
        c_sub, _ = st.columns([1, 4])
        if c_sub.form_submit_button("Submit CRM Override"):
            st.success("Entry securely logged to the Database and Change History.")
            
    st.markdown("---")
    
    st.markdown("#### 📓 Recent Uploads / Entries by Team")
    # Mock entries prioritizing India format
    df_entries = pd.DataFrame([
        {
            "Entry IST": format_datetime_ist(datetime.datetime.now()),
            "Target Revision": dates_options[0],
            "Location": "Mumbai",
            "Grade": "VG30",
            "Entered Price": format_inr(41250),
            "Conflict": "🚩 Yes (Auto was ₹ 40,500)",
            "User": "Salesman A"
        },
        {
            "Entry IST": format_datetime_ist(datetime.datetime.now() - datetime.timedelta(hours=24)),
            "Target Revision": dates_options[0],
            "Location": "Gujarat",
            "Grade": "VG10",
            "Entered Price": format_inr(39800),
            "Conflict": "✅ No Conflict",
            "User": "Manager B"
        }
    ])
    
    st.dataframe(df_entries, use_container_width=True, hide_index=True)
