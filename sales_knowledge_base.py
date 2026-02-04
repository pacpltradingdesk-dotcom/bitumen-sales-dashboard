
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
    "company": "Company Profile & Credibility",
    "product": "Product Fundamentals",
    "grades": "Bitumen Grades & Applications",
    "pricing": "Pricing & Cost Structure",
    "market": "Market Dynamics",
    "territory": "Territory Logic & Product Selection",
    "sales": "Sales Process & Customer Engagement",
    "payment": "Payment Terms & Procedures",
    "modified": "Modified Bitumen & Emulsions",
    "logistics": "Logistics & Supply Chain",
    "technical": "Technical & Consumption Metrics"
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
    }
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
        return f"Dear {customer_name},\n\nThank you for your offer. However, our current price of ₹{key_points} is the best possible workable rate given the current international crude prices. We ensure premium quality and timely delivery which cheaper alternatives may not guarantee."
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
