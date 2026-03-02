import os
import re
import json

py_files = []
for root, dirs, files in os.walk(r"c:\Users\HP\Desktop\project_bitumen sales\bitumen sales dashboard"):
    if "node_modules" in root or ".git" in root or "venv" in root:
        continue
    for f in files:
        if f.endswith(".py") and f != "india_localization.py" and f != "auto_patcher.py":
            py_files.append(os.path.join(root, f))

audit_report = {
    "us_currency_issues": [],
    "us_date_issues": [],
    "comma_grouping_issues": []
}

import_statement = "try:\n    from india_localization import format_inr, format_inr_short, format_date, format_datetime_ist, get_financial_year, get_fy_quarter\nexcept ImportError:\n    import sys\n    import os\n    sys.path.append(os.path.dirname(os.path.dirname(__file__)))\n    try:\n        from india_localization import format_inr, format_inr_short, format_date, format_datetime_ist, get_financial_year, get_fy_quarter\n    except:\n        pass\n"

for path in py_files:
    with open(path, "r", encoding="utf-8") as f:
        content = f.read()

    original_content = content
    filename = os.path.basename(path)

    # Detect US currency
    if re.search(r'\$\d+', content) or re.search(r'\$\{', content) and 'javascript' not in content.lower():
        audit_report["us_currency_issues"].append(filename)

    # Detect bad dates
    if re.search(r'%Y-%m-%d', content):
        audit_report["us_date_issues"].append(filename)

    # Detect bad commas
    if re.search(r'\{[^\}]*:,[.\d]*f\}', content):
        audit_report["comma_grouping_issues"].append(filename)

    # Fixes
    # 1. Replace '%Y-%m-%d' with '%d-%m-%Y'
    content = content.replace('%Y-%m-%d', '%d-%m-%Y')
    
    # 2. Replace {var:,.2f} with format_inr(var) -> To be safe, let's just do an Indian comma regex for streamlit metrics
    # Actually, replacing all `{:,.0f}` or `{:,.2f}` requires a bit of care.
    # Often it looks like f"₹{amount:,.0f}" ---> format_inr(amount, False)
    
    # A crude but effective way for Streamlit apps is to just swap the f"₹{val:,.2f}" to use the generic Indian regex or format function.
    # Since I don't want to break f-string logic entirely, let's do safe targeted replacements:
    # f"₹{value:,.0f}" -> f"₹ {value}" (we will let format_inr handle it inside the specific files manually if needed, but for now we'll do string replacements)
    
    content = re.sub(r'₹\s*\{([^:]+):,\.0f\}', r'{format_inr(\1)}', content)
    content = re.sub(r'₹\s*\{([^:]+):,\.2f\}', r'{format_inr(\1)}', content)
    content = re.sub(r'₹\s*\{([^:]+):,\}', r'{format_inr(\1)}', content)

    # For US dollars that were just symbols: $ -> ₹ (only if it looks like a hardcoded metric)
    content = re.sub(r'\$(\d+)', r'₹ \1', content)
    
    # Cr / Lakh standardizing
    content = content.replace("₹{", "{") # because format_inr includes ₹
    
    if content != original_content:
        # Prepend imports if we used format_inr
        if 'format_inr(' in content and 'from india_localization' not in content:
            content = import_statement + content
            
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)
        print(f"Patched: {filename}")

with open(r"c:\Users\HP\Desktop\project_bitumen sales\bitumen sales dashboard\audit_export.json", "w") as f:
    json.dump(audit_report, f, indent=4)

print("Audit and Auto-Patch Complete!")
