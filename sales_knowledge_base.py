
# PPS Anantams Corporation Pvt. Ltd.
# Bitumen Sales Training Knowledge Base
# Complete Q&A for Sales Team, AI Chatbot, and Telecalling Training

"""
This knowledge base contains all question and answers from the official 
Bitumen Sales Training Manual. It is used for:
1. Sales Person Quick Reference
2. AI Chatbot Responses
3. AI Telecalling Training Scripts
"""

# ============ KNOWLEDGE BASE STRUCTURE ============

TRAINING_SECTIONS = {
    "company":    "Company Profile & Credibility",
    "product":    "Product Fundamentals",
    "grades":     "Bitumen Grades & Applications",
    "pricing":    "Pricing & Cost Structure",
    "market":     "Market Dynamics",
    "territory":  "Territory Logic & Product Selection",
    "sales":      "Sales Process & Customer Engagement",
    "payment":    "Payment Terms & Procedures",
    "modified":   "Modified Bitumen & Emulsions",
    "logistics":  "Logistics & Supply Chain",
    "technical":  "Technical & Consumption Metrics",
    "fy26":       "FY 2025-26 Budget & Market",
    "objections": "Sales Objection Handling",
}

# ============ COMPLETE Q&A KNOWLEDGE BASE ============

KNOWLEDGE_BASE = [
    # === SECTION 1: INTRODUCTION & TRAINING OBJECTIVES ===
    {
        "section": "company",
        "question": "Who is PPS Anantams?",
        "answer": "PPS Anantams Corporation Pvt. Ltd. is a Private Limited Company headquartered in Vadodara, Gujarat. We are a trusted partner in India's bitumen supply chain with 24+ years of combined industry experience.",
        "keywords": ["company", "pps", "anantams", "who are you", "about"]
    },
     {
        "section": "company",
        "question": "What is the nature of this document?",
        "answer": "It is a comprehensive internal training manual for sales, operations, and procurement teams.",
        "keywords": ["document", "manual", "training", "what is this"]
    },
    {
        "section": "company",
        "question": "What are the main objectives of this training program?",
        "answer": "To provide product knowledge, market understanding, sales excellence techniques, and operational clarity regarding payments and logistics.",
        "keywords": ["objective", "goal", "learn", "training"]
    },
     {
        "section": "company",
        "question": "What is the company's motto?",
        "answer": "'Building Roads on Reliability - A trusted partner in India’s bitumen supply chain.'",
        "keywords": ["motto", "tagline", "slogan"]
    },

    # === SECTION 2: INDUSTRY & PRODUCT FUNDAMENTALS ===
    {
        "section": "product",
        "question": "What is bitumen?",
        "answer": "Bitumen is a black, sticky, and highly viscous petroleum product obtained exclusively from refining crude oil. It represents the heaviest fraction remaining after all lighter petroleum products (petrol, diesel, kerosene, LPG) have been extracted. It acts as the primary binding agent in asphalt concrete.",
        "keywords": ["what is bitumen", "bitumen", "definition", "explain"]
    },
    {
        "section": "product",
        "question": "Can bitumen be manufactured privately?",
        "answer": "No, bitumen cannot be manufactured by individuals or private parties outside refineries. It is exclusively a petroleum refinery product.",
        "keywords": ["manufacture", "private", "make", "produce"]
    },
    {
        "section": "product",
        "question": "Why is bitumen quality important?",
        "answer": "Poor quality leads to premature road failure. High-quality bitumen ensures waterproof protection, strong adhesive bonding, and long-lasting durability, which is critical for engineer satisfaction and project success.",
        "keywords": ["quality", "important", "why", "poor"]
    },
    {
        "section": "market",
        "question": "Which refineries produce bitumen in India?",
        "answer": "Major refineries include IOCL (Panipat, Mathura, Haldia), BPCL (Mumbai, Kochi), HPCL (Mumbai, Visakhapatnam), Reliance (Jamnagar - Largest), MRPL (Mangalore), and CPCL (Chennai).",
        "keywords": ["refineries", "iocl", "bpcl", "hpcl", "reliance", "produce"]
    },
    {
        "section": "market",
        "question": "Why does India import bitumen?",
        "answer": "India faces a structural 50% shortage. Domestic production is ~5 million MT against a demand of ~10 million MT. Imports are essential to bridge this gap, especially during peak season (Oct-March).",
        "keywords": ["import", "why", "shortage", "demand", "supply"]
    },

    # === SECTION 3: BITUMEN GRADES & APPLICATIONS ===
    {
        "section": "grades",
        "question": "What does VG stand for?",
        "answer": "VG stands for 'Viscosity Grade', a measure of bitumen's resistance to flow. Grades are standardized by BIS and IRC.",
        "keywords": ["vg", "viscosity", "grade", "meaning"]
    },
    {
        "section": "grades",
        "question": "Which grade for city roads vs highways?",
        "answer": "VG30 is standard for city roads and general construction. VG40 is used for heavy-duty highways and high-stress corridors due to higher heat resistance.",
        "keywords": ["vg30", "vg40", "city", "highway", "difference"]
    },
    {
        "section": "grades",
        "question": "Where is VG10 used?",
        "answer": "VG10 is used in cold and hilly regions (Himalayas, J&K, Northeast) where softer bitumen is required.",
        "keywords": ["vg10", "cold", "hilly", "use"]
    },
    {
        "section": "grades",
        "question": "How to estimate bitumen quantity?",
        "answer": "Quick estimate: 3 to 7 kg per square meter. DBM layer uses ~4.5%, BC layer uses ~6%. For 1km x 7m road, min ~21 MT required.",
        "keywords": ["estimate", "quantity", "calculate", "usage"]
    },

    # === SECTION 4: PRICING & COST STRUCTURE ===
    {
        "section": "pricing",
        "question": "Why do bitumen prices vary?",
        "answer": "Prices reflect base material cost + GST + freight + handling + working capital interest + transit loss + trader margin. Geography (distance from source) is the biggest factor.",
        "keywords": ["price", "vary", "different", "cost", "why"]
    },
    {
        "section": "pricing",
        "question": "What determines market price?",
        "answer": "Refinery Base Price - Discount + Transport + Conversion Cost. Discounts vary by refinery competition.",
        "keywords": ["pricing", "formula", "determine"]
    },
    {
        "section": "pricing",
        "question": "What is included in your quote?",
        "answer": "We quote basic material cost at loading point. Exclusions (at actuals): GST, Freight, Loading/Unloading, Tolls, Detention.",
        "keywords": ["quote", "include", "exclude", "terms"]
    },

    # === SECTION 5: MODIFIED BITUMEN & EMULSIONS ===
    {
        "section": "modified",
        "question": "What is Bituminous Emulsion?",
        "answer": "A liquid mix of bitumen and water applied cold (no heating) for prime/tack coats and patchwork.",
        "keywords": ["emulsion", "liquid", "cold", "patchwork"]
    },
    {
        "section": "modified",
        "question": "Difference between CRMB and PMB?",
        "answer": "CRMB (Crumb Rubber) uses recycled rubber for elasticity (Highways). PMB (Polymer Modified) uses polymers for premium stability (Airports, Expressways).",
        "keywords": ["crmb", "pmb", "difference", "modified"]
    },
    {
        "section": "modified",
        "question": "Do you supply modified bitumen?",
        "answer": "We specialize in VG grades but supply Modified Bitumen through partnerships with reputed manufacturers on request.",
        "keywords": ["supply", "modified", "available"]
    },

    # === SECTION 6: COMPANY CREDIBILITY ===
    {
        "section": "company",
        "question": "Why choose PPS Anantams?",
        "answer": "24+ years experience, PAN-India presence (Mundra, Mumbai, Karwar, Haldia), 100% banking transactions (Debt-free), and purely professional execution.",
        "keywords": ["why us", "choose", "advantage", "benefit"]
    },
    {
        "section": "company",
        "question": "Is the company financially stable?",
        "answer": "Yes. We are 100% debt-free, use own capital, and do not rely on bank limits/LCs. This ensures supply reliability.",
        "keywords": ["financial", "stable", "money", "credit"]
    },

    # === SECTION 7: TERRITORY LOGIC ===
    {
        "section": "territory",
        "question": "When to sell Drum vs Bulk?",
        "answer": "Sell Bulk in South India (high refinery density). Sell Drum in North/Central India and 'Centre Zones' (Nagpur, Indore) where bulk freight is high. Use Kandla-Mundra hub for North India.",
        "keywords": ["drum", "bulk", "when", "territory", "logic"]
    },
    {
        "section": "territory",
        "question": "Why Kandla-Mundra for Drum?",
        "answer": "Lowest freight rates allowing 1500-2000km reach. Covers Guj, Raj, MP, UP, Delhi, Punjab.",
        "keywords": ["kandla", "mundra", "hub", "why"]
    },

    # === SECTION 8: SALES PROCESS ===
    {
        "section": "sales",
        "question": "Correct sales opening?",
        "answer": "Professional tone: 'Hello, I'm [Name] from PPS Anantams. Calling regarding bitumen supply as discussed.' Avoid begging ('Give me one chance').",
        "keywords": ["opening", "call", "script", "start"]
    },
    {
        "section": "sales",
        "question": "Questions before quoting?",
        "answer": "1. Location? 2. Distance from Port? 3. Monthly Qty? 4. Storage (Tank or Decanter)? 5. Final Use?",
        "keywords": ["ask", "questions", "before", "quote"]
    },
    {
        "section": "sales",
        "question": "How to handle 'Price High' objection?",
        "answer": "Explain that refined base prices are similar; difference is logistics. Offer to calculate exact landed cost to show value.",
        "keywords": ["price high", "expensive", "objection"]
    },

    # === SECTION 9: LOGISTICS & PAYMENT ===
    {
        "section": "payment",
        "question": "Payment terms for Bulk?",
        "answer": "100% Advance mandatory. Risk of diversion after tanker leaves is too high.",
        "keywords": ["bulk", "payment", "advance"]
    },
    {
        "section": "payment",
        "question": "Payment terms for Drum?",
        "answer": "Advance preferred, but can allow payment after loading (with weight slip confirmation) for secure routes.",
        "keywords": ["drum", "payment", "terms"]
    },
    {
        "section": "logistics",
        "question": "What is Transit Loss?",
        "answer": "Standard material loss during handling/transport. factored into pricing.",
        "keywords": ["transit", "loss"]
    },
    
    # === SECTION 10: TECHNICAL DEEP DIVE ===
    {
        "section": "product",
        "question": "How is Bitumen manufactured in an Oil Company?",
        "answer": "It is produced through the fractional distillation of crude oil. In the vacuum distillation column, lighter fractions (petrol, diesel, kerosene) are boiled off. The heaviest residue at the bottom is processed (via air blowing or blending) to create specific viscosity grades like VG30/VG40.",
        "keywords": ["manufacturing", "oil company", "distillation", "process", "made"]
    },
    {
        "section": "grades",
        "question": "Which Bitumen grade corresponds to which weather?",
        "answer": "1. VG10: Cold/Hilly regions (Prevents cold cracking).\n2. VG30: Moderate/Standard climates (Most India).\n3. VG40: Hot climates & Heavy Traffic (Resists melting/rutting).",
        "keywords": ["weather", "climate", "temperature", "selection", "vg10", "vg30", "vg40"]
    },
    {
        "section": "technical",
        "question": "How much Bitumen is used per Square Meter (SQM)?",
        "answer": "1. Tack Coat: 0.2 - 0.3 kg/sqm.\n2. Prime Coat: 0.6 - 1.0 kg/sqm.\n3. Road Layer (50mm DBM): Approx 5.5 - 6.0 kg/sqm (at 4.5% content).",
        "keywords": ["consumption", "sqm", "coverage", "quantity", "usage"]
    },
    {
        "section": "technical",
        "question": "How is Bitumen actually applied on roads?",
        "answer": "It is heated to 150-160°C to liquefy. It is then mixed with hot aggregates (stone/sand) in a Hot Mix Plant. This 'Hot Mix' is transported via dumpers, laid by a Paver finisher, and compacted by rollers before cooling.",
        "keywords": ["application", "how used", "mixing", "process", "road"]
    },

    # === SECTION 12: FY 2025-26 BUDGET & MARKET ===
    {
        "section": "fy26",
        "question": "What is India's road infrastructure budget for FY 2025-26?",
        "answer": "MORTH has allocated ₹11.11 lakh crore for road infrastructure in FY 2025-26 — the highest ever. PM GatiShakti targets 50 km/day highway construction. This directly drives bitumen demand with an estimated 8.5–10.5 million MT requirement nationally.",
        "keywords": ["budget", "fy26", "fy2026", "morth", "infrastructure", "allocation", "11.11", "lakh crore"]
    },
    {
        "section": "fy26",
        "question": "What is the expected bitumen demand for FY 2025-26?",
        "answer": "India's bitumen demand for FY 2025-26 is estimated at 8.5–10.5 million MT. Domestic production covers approximately 5 MT, leaving 4–5 MT to be imported. Peak demand months: October–March (post-monsoon construction window). Gujarat, Maharashtra, UP, and Rajasthan are the top-consuming states.",
        "keywords": ["demand", "fy26", "2026", "million mt", "import gap", "consumption"]
    },
    {
        "section": "fy26",
        "question": "What is PM GatiShakti and how does it affect bitumen demand?",
        "answer": "PM GatiShakti is a national master plan for multi-modal connectivity launched in 2021. It targets 50 km/day highway construction and integrates road, rail, port, and air connectivity. Higher highway construction pace directly increases bitumen offtake — every 1 km of 4-lane highway requires 140–160 MT of bitumen.",
        "keywords": ["gatishakti", "gati shakti", "50 km", "highway", "nhai", "infrastructure plan"]
    },
    {
        "section": "fy26",
        "question": "Which states consume the most bitumen in India?",
        "answer": "Top bitumen-consuming states: (1) Uttar Pradesh — largest road network, NHAI projects; (2) Maharashtra — Mumbai-Nagpur, coastal highway; (3) Rajasthan — large highway projects, state PWD; (4) Gujarat — port connectivity, industrial corridors; (5) Madhya Pradesh — Bharatmala Phase 1 projects. Gujarat is our primary territory and a top-3 consumer.",
        "keywords": ["which state", "top state", "highest demand", "gujarat", "maharashtra", "up", "rajasthan"]
    },
    {
        "section": "fy26",
        "question": "What is the NHAI budget for FY 2025-26?",
        "answer": "NHAI has been allocated approximately ₹1.68 lakh crore for highway construction in FY 2025-26. NHAI is targeting construction of 12,000+ km of national highways. This is the single largest driver of bulk bitumen demand from institutional buyers.",
        "keywords": ["nhai", "national highway", "budget", "highway construction", "fy26"]
    },

    # === SECTION 13: SALES OBJECTION HANDLING ===
    {
        "section": "objections",
        "question": "Customer says: Your price is too high compared to local supplier.",
        "answer": "Response: 'Sir, I understand the comparison. But let me show you the full cost picture. Local suppliers often quote low but hide: (1) GST ITC mismatch risk — if supplier is non-compliant, you lose 18% credit; (2) Quality risk — substandard VG grade causes road failure, leading to penalty deductions; (3) No e-invoice — your audit risk increases. Our price includes IS 73-certified material, e-invoice, full ITC chain, and on-time delivery. The net cost to you is actually lower when you factor in these protections.'",
        "keywords": ["price too high", "expensive", "local supplier cheaper", "competitor cheaper", "objection", "cost"]
    },
    {
        "section": "objections",
        "question": "Customer says: I will buy directly from IOCL.",
        "answer": "Response: 'IOCL direct supply is excellent for large captive consumers with pre-approved lifting schedules. For project-based contractors, IOCL requires: (1) Pre-registration and credit approval (45–90 days); (2) Full advance payment or bank guarantee; (3) Fixed delivery windows — no spot flexibility; (4) Minimum lifting quantity per month. We offer same-day confirmation, 30-day credit facility, flexible quantity from 10 MT, and custom delivery scheduling — critical for project cash flow management.'",
        "keywords": ["iocl direct", "buy direct", "refinery direct", "iocl", "bpcl direct", "objection"]
    },
    {
        "section": "objections",
        "question": "Customer says: Import bitumen quality is risky.",
        "answer": "Response: 'This is a valid concern. Our import supply is quality-protected at three levels: (1) Origin lab test — COA (Certificate of Analysis) from Iraqi refinery before loading; (2) Port QC — independent inspection at Kandla/Mundra by accredited lab (Bureau Veritas / SGS); (3) Pre-delivery test — we can arrange BIS-accredited lab report at your site. All material meets IS 73:2013 specification for VG-30. We provide full documentation trail.'",
        "keywords": ["import quality", "risky", "bad quality", "is 73", "certificate", "objection", "quality concern"]
    },
    {
        "section": "objections",
        "question": "Customer says: I will wait for bitumen prices to fall.",
        "answer": "Response: 'Bitumen prices are linked to Brent crude and USD/INR — two factors that are currently moving against a price drop. Our 24-month forecast model (1st & 16th cycle) shows prices stable-to-upward for the next 3–4 months. Additionally, waiting creates project delay risk: for every 10 days of delay in bitumen arrival, a typical highway project loses 0.8–1.2 km of laying opportunity. Shall I show you the forecast data?'",
        "keywords": ["wait", "price will fall", "prices drop", "hold off", "delay purchase", "objection"]
    },
    {
        "section": "objections",
        "question": "Customer says: Other dealers give 60-day credit.",
        "answer": "Response: 'We understand credit terms are important. Our standard terms are 30 days for established accounts. For 45+ day credit, we require: (1) 6-month purchase history; (2) Credit limit approval by our Finance team (usually 3–5 days); (3) PDC or bank guarantee. The dealers offering 60-day credit typically build a 2–3% higher margin into their price to offset the risk. Net-net, you may pay more. Shall we calculate the effective cost comparison?'",
        "keywords": ["60 day credit", "45 day", "payment terms", "credit period", "other dealers", "objection"]
    },
    {
        "section": "objections",
        "question": "Customer says: Why 100% advance payment?",
        "answer": "Response: 'For first-time orders or new accounts, 100% advance protects both parties — it confirms your order is firm and allows us to reserve allocation from the refinery/port stock. For repeat customers with good payment history, we move to 50% advance + 50% on delivery, and eventually to credit terms. Think of it as building a banking relationship — creditworthiness is established through transaction history. We want to grow with you.'",
        "keywords": ["advance payment", "100% advance", "why advance", "upfront payment", "prepayment", "objection"]
    },

    # === SECTION 14: ADVANCED PRICING ===
    {
        "section": "pricing",
        "question": "How is import parity price calculated for bitumen?",
        "answer": "Import parity = FOB price (Iraq/UAE, USD/MT) × USD/INR exchange rate + Ocean freight ($30–45/MT) × USD/INR + Insurance (0.5% of CIF value) + Customs duty (2.5% of CIF) + Port charges ₹75–150/MT + GST 18% of (CIF + Customs + Port). At Brent ~$75/bbl, FOB Iraq bitumen is approximately $380–400/MT. Typical landed cost at Kandla: ₹47,000–49,500/MT before dealer margin.",
        "keywords": ["import parity", "landed cost", "how calculated", "fob", "cif", "customs duty", "formula"]
    },
    {
        "section": "pricing",
        "question": "How does Brent crude price affect Indian bitumen price?",
        "answer": "Every $1 increase in Brent crude price increases Indian bitumen price by approximately ₹100–120/MT. This is because: (1) Bitumen feedstock cost is directly linked to crude; (2) PSU refineries revise prices to maintain margins; (3) Import parity recalculates with new FOB price. Example: Brent moves from $70 to $75 (+$5) → bitumen price rises by ₹500–600/MT at PSU level and ₹600–700/MT for import material (additional FX exposure).",
        "keywords": ["brent effect", "crude price impact", "how crude affects", "brent to bitumen", "price calculation"]
    },
    {
        "section": "pricing",
        "question": "How does USD/INR exchange rate affect bitumen prices?",
        "answer": "Every ₹1 depreciation in USD/INR increases imported bitumen landed cost by approximately ₹75–90/MT. Example: USD/INR moves from 84 to 85 (₹1 weaker) → import cost rises ₹380 × 1 = ₹380/MT at FOB level, plus freight escalation ≈ ₹35/MT × 1 → total impact ~₹415/MT on CIF, after adding duties/GST ≈ ₹75–90/MT net impact on final price. PSU prices are less directly affected but follow import parity trend.",
        "keywords": ["usd inr", "exchange rate", "rupee depreciation", "forex effect", "dollar rate impact"]
    },
    {
        "section": "pricing",
        "question": "What is decanting and when is it cheaper than buying bulk bitumen?",
        "answer": "Decanting is the process of converting drum bitumen into bulk liquid form using a decanter machine (steam-heated tank). It is cheaper than bulk when: (1) Destination is far from a bulk terminal (>600 km); (2) Order quantity is small (<200 MT — below minimum bulk tanker load); (3) Site has no bulk storage facility. Typical decanting cost: ₹450–600/MT conversion + drum price premium of ₹2,000–3,500/MT over bulk. It becomes viable when bulk transport cost exceeds drum + decanting cost.",
        "keywords": ["decanting", "decanter", "drum to bulk", "when cheaper", "drum conversion", "bulk vs drum"]
    },
    {
        "section": "pricing",
        "question": "What is the 1st and 16th pricing cycle for bitumen?",
        "answer": "PSU refineries (IOCL, BPCL, HPCL) revise bitumen prices on the 1st and 16th of every month, creating 24 pricing events per year. Notification is issued 2–3 days before effective date. Import parity players adjust simultaneously. Key implication for sales: (1) If price is expected to rise on 1st, push customers to book before month-end; (2) If prices fall on 16th, offer post-revision pricing for future deliveries; (3) Our dashboard's price prediction shows the next 24 months of 1st/16th forecasts.",
        "keywords": ["1st 16th", "pricing cycle", "revision cycle", "monthly revision", "when prices change"]
    },

    # === SECTION 15: TECHNICAL DEPTH ===
    {
        "section": "technical",
        "question": "What is PMB (Polymer Modified Bitumen) and when is it used?",
        "answer": "PMB is VG-40 base bitumen modified with SBS (Styrene Butadiene Styrene) or EVA polymer at 3–4% dosage. Benefits: (1) Higher softening point → better rutting resistance at high temperatures; (2) Lower ductility at low temperatures → better crack resistance; (3) Extended road life (2–3× vs standard VG-30). Required by IRC SP:53 for: highways with >10 mSA traffic loading, bus depots, toll plazas, bridge deck waterproofing. Price premium: ₹8,000–12,000/MT over VG-30.",
        "keywords": ["pmb", "polymer modified", "sbs", "eva", "modified bitumen", "rutting", "when use pmb"]
    },
    {
        "section": "technical",
        "question": "How much bitumen is needed per kilometre of road?",
        "answer": "Approximate consumption per km: (1) 4-lane highway (7m wide, 50mm thickness DBM + 25mm BC): 140–160 MT/km; (2) 2-lane road (3.5m wide, 40mm thickness): 55–70 MT/km; (3) Urban road (surface dressing, 10mm chip): 8–12 MT/km; (4) Pothole repair / patch work: 0.5–2 MT/km. Actual consumption varies by mix design, traffic loading, and compaction efficiency. Use these for quick project demand estimation.",
        "keywords": ["bitumen per km", "consumption per km", "how much bitumen", "quantity per km", "mt per km", "highway consumption"]
    },
    {
        "section": "technical",
        "question": "What is the difference between VG-30 and VG-40?",
        "answer": "VG-30 (Viscosity Grade 30): Kinematic viscosity 2400–3600 cSt at 60°C. Used for: normal traffic roads, city roads, feeder roads, rural highways. Most common grade in India (~70% of market). VG-40 (Viscosity Grade 40): Kinematic viscosity 3200–4800 cSt at 60°C. Stiffer, higher softening point. Used for: heavy traffic highways (>10 mSA), bus terminals, urban arterials in hot climates. VG-40 premium: ₹1,500–2,500/MT over VG-30. Recommendation: Use VG-30 as default; upgrade to VG-40 for heavy commercial vehicle routes.",
        "keywords": ["vg30 vs vg40", "difference vg30 vg40", "which grade", "vg40 uses", "viscosity grade"]
    },
    {
        "section": "technical",
        "question": "What is CRMB (Crumb Rubber Modified Bitumen)?",
        "answer": "CRMB is bitumen modified with crumb rubber from waste tyres at 15–20% dosage. Mandated by IRC SP:107 for surface course on National Highways since 2018. Benefits: (1) Improved elasticity and fatigue resistance; (2) Reduces road noise; (3) Environmentally friendly — uses waste tyres. Grades: CRMB-50, CRMB-55, CRMB-60 (softening point). Price: ₹5,000–8,000/MT premium over VG-30. Note: Mandated for NHS in India; many state PWDs are also adopting it.",
        "keywords": ["crmb", "crumb rubber", "rubber modified", "waste tyre", "crmb 55", "crmb 60", "irc 107"]
    },
    {
        "section": "technical",
        "question": "How do you read a bitumen test report (COA)?",
        "answer": "Key parameters in a bitumen COA (Certificate of Analysis): (1) Penetration at 25°C (dmm) — VG-30 range: not specified (consistency grade); (2) Softening Point (°C) — VG-30 minimum: 47°C; higher is better for hot climates; (3) Viscosity at 60°C (Poise) — VG-30: 2400–3600 Poise; (4) Ductility at 27°C (cm) — minimum 75 cm; (5) Flash Point (°C) — minimum 220°C (safety); (6) Specific Gravity — ~1.01–1.05. If softening point < 47°C or ductility < 75 cm, the batch fails IS 73:2013 spec.",
        "keywords": ["test report", "coa", "certificate of analysis", "penetration", "softening point", "viscosity", "ductility", "is 73"]
    },

    # === SECTION 16: TERRITORY & LOGISTICS ===
    {
        "section": "territory",
        "question": "Which PSU refinery serves Gujarat and Rajasthan?",
        "answer": "Primary: IOCL Koyali (Vadodara, Gujarat) — closest to our HQ, best logistics for Central/North Gujarat. Secondary: IOCL Mathura (UP) — serves North Rajasthan, Haryana. BPCL Mumbai (Mahul) — serves South Gujarat, Surat corridor. For import: Kandla Port (Kutch) and Mundra Port (Kutch) serve all of Gujarat and Rajasthan with competitive landed costs. PPS Anantams is positioned optimally near IOCL Koyali — shortest supply chain for Gujarat buyers.",
        "keywords": ["gujarat refinery", "rajasthan refinery", "which refinery", "iocl koyali", "territory", "state refinery"]
    },
    {
        "section": "territory",
        "question": "Which ports handle bitumen imports for South India?",
        "answer": "South India import ports: (1) Ennore (Chennai) — Tamil Nadu, Andhra Pradesh; (2) Vizag (Visakhapatnam) — Andhra Pradesh, Odisha; (3) Mangalore (MRPL terminal) — Karnataka, Kerala. West coast: (4) Kandla — Gujarat, Rajasthan; (5) Mundra — Gujarat, MP; (6) JNPT (Nhava Sheva) — Maharashtra, South India secondary. East coast: (7) Paradip — Odisha, WB; (8) Haldia — West Bengal, Bihar.",
        "keywords": ["south india ports", "import port", "ennore", "vizag", "mangalore", "which port", "south india bitumen"]
    },
    {
        "section": "territory",
        "question": "When to recommend drum bitumen vs bulk bitumen to a customer?",
        "answer": "Recommend DRUMS when: (1) Order < 100 MT — below minimum bulk tanker capacity; (2) Customer site has no bulk storage/decanting facility; (3) Remote location (>700 km from port/refinery) where bulk tanker logistics are complex; (4) Customer needs multiple small deliveries spread over months. Recommend BULK when: (1) Order > 200 MT at one delivery; (2) Customer has storage tank or decanter; (3) Customer near major city or port. Drum premium: ₹2,000–3,500/MT — if decanting cost < this, bulk wins.",
        "keywords": ["drum vs bulk", "when drum", "drum supply", "bulk supply", "recommend drum", "small order"]
    },
    {
        "section": "territory",
        "question": "How does distance affect the PSU vs import feasibility decision?",
        "answer": "Rule of thumb: (1) Within 400 km of a PSU refinery → PSU supply is cheaper (lower transport cost offsets PSU premium); (2) 400–700 km → depends on specific refinery and port prices — use our Feasibility page for exact comparison; (3) > 700 km from any PSU + within 300 km of a port → import is usually cheaper. Gujarat is unique: near both IOCL Koyali AND Kandla/Mundra ports, creating strong competition. Use the dashboard's Feasibility page to enter exact destination and get real-time rank comparison.",
        "keywords": ["distance feasibility", "psu vs import", "when import cheaper", "when psu cheaper", "feasibility", "break even distance"]
    },
]

# ============ CHATBOT FUNCTIONS ============

import re
from difflib import SequenceMatcher

def normalize_text(text):
    """Normalize text for matching."""
    text = text.lower()
    text = re.sub(r'[^\w\s]', '', text)
    return text

def calculate_similarity(text1, text2):
    """Calculate similarity between two texts."""
    return SequenceMatcher(None, normalize_text(text1), normalize_text(text2)).ratio()

def find_best_match(user_query, min_similarity=0.3):
    """Find the best matching Q&A for a user query."""
    user_query_normalized = normalize_text(user_query)
    best_match = None
    best_score = 0
    
    for qa in KNOWLEDGE_BASE:
        # Check question similarity
        q_similarity = calculate_similarity(user_query, qa['question'])
        
        # Check keyword matches
        keyword_score = 0
        for keyword in qa.get('keywords', []):
            if keyword.lower() in user_query_normalized:
                keyword_score += 0.2
        
        # Combined score
        total_score = q_similarity + keyword_score
        
        if total_score > best_score:
            best_score = total_score
            best_match = qa
    
    if best_score >= min_similarity:
        return best_match, best_score
    return None, 0

def get_chatbot_response(user_query):
    """Get chatbot response for a user query."""
    match, score = find_best_match(user_query)
    
    if match:
        return {
            "found": True,
            "answer": match['answer'],
            "question": match['question'],
            "section": TRAINING_SECTIONS.get(match['section'], match['section']),
            "confidence": min(score * 100, 100)
        }
    else:
        return {
            "found": False,
            "answer": "I'm sorry, I couldn't find a specific answer to that question. Please contact our sales team at +91 94482 81224 or email sales.ppsanantams@gmail.com for assistance.",
            "question": user_query,
            "section": "general",
            "confidence": 0
        }

def get_section_questions(section_key):
    """Get all questions for a specific section."""
    return [qa for qa in KNOWLEDGE_BASE if qa['section'] == section_key]

def get_quick_reference(topic):
    """Get quick reference for common topics."""
    quick_refs = {
        "grades": "VG10: Cold regions | VG30: Standard roads (most common) | VG40: Heavy highways",
        "ports": "Kandla (primary), Mundra, Mumbai (200km radius only), Karwar (South)",
        "payment": "Bulk: 100% advance | Drum: After loading (with confirmation)",
        "contact": "+91 94482 81224 | sales.ppsanantams@gmail.com | Vadodara, Gujarat",
        "drums": "180 kg imported drums (vs 154 kg domestic) | VG30 & VG40 available",
        "territory": "North/Central: Drum | South: Bulk | Centre Zones: Drum to decanters"
    }
    return quick_refs.get(topic, None)

# ============ AI ASSISTANT FUNCTIONS ============

def polish_email(rough_text, tone="Professional"):
    """
    Simulates AI Email Polishing.
    In production, this would call OpenAI API.
    """
    templates = {
        "Professional": f"Dear Sir/Madam,\n\n{rough_text}\n\nWe look forward to your positive response.\n\nBest Regards,\nPPS Anantams Sales Team",
        "Urgent": f"URGENT ATTENTION REQUIRED\n\nDear Partner,\n\n{rough_text}\n\nPlease treat this as a priority.\n\nRegards,\nPPS Anantams",
        "Friendly": f"Hi Team,\n\nHope you're doing well!\n\n{rough_text}\n\nThanks & Regards,\n[Your Name]"
    }
    # Simple wrapper for now
    return templates.get(tone, rough_text)

def generate_custom_reply(customer_name, topic, key_points):
    """
    Generates a contextual reply based on topic.
    """
    if topic == "Price Negotiation":
        return f"Dear {customer_name},\n\nThank you for your offer. However, our current price of {key_points} is the best possible workable rate given the current international crude prices. We ensure premium quality and timely delivery which cheaper alternatives may not guarantee."
    elif topic == "Supply Delay":
        return f"Dear {customer_name},\n\nWe apologize for the delay in the vehicle reaching your site. This is due to {key_points}. We are constantly tracking the vehicle and it should reach by [Time]."
    return f"Dear {customer_name},\n\nRegarding {topic}: {key_points}\n\nLet us know how to proceed."

# ============ TELECALLING SCRIPTS ============

TELECALLING_SCRIPTS = {
    "intro": """
Hello, this is [Your Name] calling from PPS Anantams Corporation, Vadodara. 
We are importers and suppliers of VG-grade bitumen. 
I wanted to briefly introduce our company - is this a good time for 2 minutes?
""",
    
    "credibility": """
Sir, we have 24+ years of combined experience in bitumen trading.
We import through Kandla and Mumbai ports and have regular supplies to major contractors in your state.
We are a debt-free, 100% banking transaction company with zero complaints.
""",
    
    "qualification": """
Before I share pricing, may I know:
1. What is your approximate monthly requirement in MT?
2. Do you have a storage tank or work with a decanter?
3. Are you using this for your own projects or trading?
""",
    
    "closing": """
Based on our discussion, I can share today's workable price for your location.
If it matches your expectation, we can plan a trial supply.
Shall I send you the rate card on WhatsApp?
""",
    
    "objection_price": """
I understand price is important. 
Actually, base prices are similar across India - the difference comes from logistics costs.
Let me calculate the exact landed cost for your location - 
sometimes Kandla route works better than Mumbai even for your area.
""",
    
    "objection_credit": """
We operate on advance payment basis which allows us to offer competitive rates.
Many large contractors prefer this as it ensures clean documentation and stable supply.
For the first order, if you're comfortable, we can start with a smaller quantity as trial.
""",
    
    "follow_up": """
Hello Sir, this is [Name] from PPS Anantams following up on our bitumen discussion.
I wanted to check if you've had a chance to review the rates I shared.
Is there any upcoming requirement we can support?
"""
}

def get_telecalling_script(scenario):
    """Get telecalling script for a scenario."""
    return TELECALLING_SCRIPTS.get(scenario, TELECALLING_SCRIPTS['intro'])

# ============ EXPORT FOR DASHBOARD ============

def get_all_sections():
    """Get all training sections."""
    return TRAINING_SECTIONS

def get_knowledge_count():
    """Get count of Q&A pairs."""
    return len(KNOWLEDGE_BASE)
