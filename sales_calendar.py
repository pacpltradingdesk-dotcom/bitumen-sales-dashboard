
# Sales Calendar - Weather Seasons, Holidays, Festivals for Bitumen Sales Planning
# Bitumen NOT purchased during: Rainy Season (Jun-Sep), Extreme Cold (Dec-Jan in North)
# INCLUDES comprehensive Festival "Atmosphere" Duration (Holidays Mood)

import datetime
import calendar
from typing import Dict, List

# ============ WEATHER SEASONS BY CITY ============
# Peak Season = Best time for bitumen sales (dry, construction weather)
# Off Season = Low/No sales (rain, extreme cold)

CITY_SEASONS = {
    # Gujarat Cities - Monsoon June-September
    "Ahmedabad": {
        "peak": [(2, 5), (10, 11)],  # Feb-May, Oct-Nov
        "moderate": [(1, 1), (12, 12)],  # Jan, Dec (mild winter)
        "off": [(6, 9)],  # June-Sep (Monsoon)
        "monsoon_start": 6, "monsoon_end": 9,
        "remarks": "Peak construction: Feb-May. Monsoon pause: Jun-Sep."
    },
    "Vadodara": {
        "peak": [(2, 5), (10, 11)],
        "moderate": [(1, 1), (12, 12)],
        "off": [(6, 9)],
        "monsoon_start": 6, "monsoon_end": 9,
        "remarks": "Same as Ahmedabad. Heavy rains July-Aug."
    },
    "Surat": {
        "peak": [(2, 5), (10, 11)],
        "moderate": [(1, 1), (12, 12)],
        "off": [(6, 9)],
        "monsoon_start": 6, "monsoon_end": 9,
        "remarks": "Coastal monsoon - very heavy rainfall July."
    },
    "Rajkot": {
        "peak": [(2, 5), (10, 11)],
        "moderate": [(1, 1), (12, 12)],
        "off": [(6, 9)],
        "monsoon_start": 6, "monsoon_end": 9,
        "remarks": "Hot summer, monsoon rains moderate."
    },
    "Kutch": {
        "peak": [(10, 3)],  # Oct-March (best season)
        "moderate": [(4, 5)],  # April-May (very hot)
        "off": [(6, 9)],  # Monsoon
        "monsoon_start": 6, "monsoon_end": 9,
        "remarks": "Best Oct-Mar. Rann area very hot Apr-May."
    },
    
    # Maharashtra Cities
    "Mumbai": {
        "peak": [(10, 5)],  # Oct-May
        "moderate": [],
        "off": [(6, 9)],  # Heavy monsoon
        "monsoon_start": 6, "monsoon_end": 9,
        "remarks": "VERY heavy monsoon Jun-Sep. No road work possible."
    },
    "Pune": {
        "peak": [(10, 5)],
        "moderate": [],
        "off": [(6, 9)],
        "monsoon_start": 6, "monsoon_end": 9,
        "remarks": "Heavy Western Ghats rainfall Jun-Sep."
    },
    "Nagpur": {
        "peak": [(10, 5)],
        "moderate": [],
        "off": [(6, 9)],
        "monsoon_start": 6, "monsoon_end": 9,
        "remarks": "Central India monsoon. Extreme heat May."
    },
    "Nashik": {
        "peak": [(10, 5)],
        "moderate": [],
        "off": [(6, 9)],
        "monsoon_start": 6, "monsoon_end": 9,
        "remarks": "Wine country. Moderate climate year-round."
    },
    
    # Madhya Pradesh
    "Indore": {
        "peak": [(10, 5)],
        "moderate": [],
        "off": [(6, 9)],
        "monsoon_start": 6, "monsoon_end": 9,
        "remarks": "Central India weather pattern."
    },
    "Bhopal": {
        "peak": [(10, 5)],
        "moderate": [],
        "off": [(6, 9)],
        "monsoon_start": 6, "monsoon_end": 9,
        "remarks": "Lake city. Moderate monsoon."
    },
    
    # Rajasthan - Desert Climate
    "Jaipur": {
        "peak": [(10, 3)],  # Oct-March (Winter best)
        "moderate": [(4, 5)],  # Very hot
        "off": [(6, 9)],  # Monsoon (less rain but still off)
        "monsoon_start": 7, "monsoon_end": 9,
        "remarks": "Hot semi-arid. High construction during winter."
    },
    "Udaipur": {
        "peak": [(10, 3)],
        "moderate": [(4, 5)],
        "off": [(6, 9)],
        "monsoon_start": 7, "monsoon_end": 9,
        "remarks": "Hilly terrain. Good winter market."
    },
    
    # North India
    "Delhi": {
        "peak": [(2, 4), (10, 11)], # Feb-Apr, Oct-Nov
        "moderate": [(1, 1), (12, 12)], # Pollution/Winter bans
        "off": [(6, 9)],
        "monsoon_start": 7, "monsoon_end": 9,
        "remarks": "⚠️ Construction often banned Nov-Dec due to pollution."
    },
    "Ludhiana": {
        "peak": [(3, 6), (9, 11)],
        "moderate": [],
        "off": [(12, 2), (7, 8)], # Winter off (fog/cold) + Rain
        "monsoon_start": 7, "monsoon_end": 8,
        "remarks": "Very cold Dec-Jan. Fog disrupts logistics."
    },
    
    # East India
    "Kolkata": {
        "peak": [(11, 4)],
        "moderate": [],
        "off": [(6, 9)],  # Heavy monsoon
        "monsoon_start": 6, "monsoon_end": 10,
        "remarks": "Very heavy monsoon Jun-Oct. Humid."
    },
    "Patna": {
        "peak": [(10, 5)],
        "moderate": [],
        "off": [(6, 9)],
        "monsoon_start": 6, "monsoon_end": 9,
        "remarks": "Gangetic plain - floods in monsoon."
    },
    "Ranchi": {
        "peak": [(10, 5)],
        "moderate": [],
        "off": [(6, 9)],
        "monsoon_start": 6, "monsoon_end": 9,
        "remarks": "Hill station - moderate climate."
    },
    "Guwahati": {
        "peak": [(10, 4)],  # Shorter peak
        "moderate": [(5, 5)],
        "off": [(6, 9)],  # Very heavy rainfall
        "monsoon_start": 5, "monsoon_end": 9,
        "remarks": "⚠️ Very heavy rainfall May-Sep. Short window."
    },
    
    # South India
    "Hyderabad": {
        "peak": [(10, 5)],
        "moderate": [],
        "off": [(6, 9)],
        "monsoon_start": 6, "monsoon_end": 10,
        "remarks": "Both SW and NE monsoon. Oct rains."
    },
    "Visakhapatnam": {
        "peak": [(12, 5)],  # Extended peak
        "moderate": [],
        "off": [(6, 11)],  # Extended monsoon
        "monsoon_start": 6, "monsoon_end": 11,
        "remarks": "⚠️ Cyclone season Oct-Nov. Coastal rains."
    },
    "Bangalore": {
        "peak": [(1, 5), (11, 12)],
        "moderate": [(10, 10)],
        "off": [(6, 9)],
        "monsoon_start": 6, "monsoon_end": 9,
        "remarks": "Pleasant climate. Moderate monsoon."
    },
    "Mangalore": {
        "peak": [(10, 5)],
        "moderate": [],
        "off": [(6, 9)],  # Very heavy coastal monsoon
        "monsoon_start": 6, "monsoon_end": 9,
        "remarks": "⚠️ VERY heavy coastal rainfall Jun-Sep."
    },
    "Mysore": {
        "peak": [(1, 5), (10, 12)],
        "moderate": [],
        "off": [(6, 9)],
        "monsoon_start": 6, "monsoon_end": 9,
        "remarks": "Pleasant climate similar to Bangalore."
    },
    "Chennai": {
        "peak": [(1, 5)],  # Before SW monsoon
        "moderate": [(6, 9)],  # SW monsoon is lighter
        "off": [(10, 12)],  # NE Monsoon is heavy
        "monsoon_start": 10, "monsoon_end": 12,
        "remarks": "⚠️ NE Monsoon Oct-Dec is HEAVY. Different pattern."
    },
    "Kochi": {
        "peak": [(1, 5), (10, 12)],
        "moderate": [],
        "off": [(6, 9)],  # SW Monsoon very heavy
        "monsoon_start": 6, "monsoon_end": 9,
        "remarks": "⚠️ Very heavy SW monsoon. Tourism peak Dec-Mar."
    },
}

# ============ NATIONAL HOLIDAYS 2026 (India) ============
NATIONAL_HOLIDAYS_2026 = [
    (1, 26, "Republic Day 🇮🇳", "1 day"),
    (8, 15, "Independence Day 🇮🇳", "1 day"),
    (10, 2, "Gandhi Jayanti 🇮🇳", "1 day"),
    (5, 1, "Labour Day (Majdoor Divas)", "1 day"),
]

# ============ EXTENSIVE MAJOR FESTIVALS 2026 (Atmosphere & Duration) ============
# Format: (Month, Day, Name, Atmosphere_Duration_Days)
# Atmosphere Duration: How long the market mood is affected (before/after)

MAJOR_FESTIVALS_2026 = [
    (1, 14, "Makar Sankranti / Pongal 🪁", "3 days (Market slow)"),
    (1, 26, "Republic Day 🇮🇳", "1 day"),
    (2, 16, "Mahashivratri 🕉️", "1 day"),
    (3, 4, "Holi (Holika Dahan) 🔥", "2 days"),
    (3, 5, "Holi (Rangwali) 🎨", "3-5 days (North India closed)"), 
    (3, 20, "Gudi Padwa / Ugadi / Cheti Chand", "1-2 days"),
    (3, 30, "Ram Navami 🚩", "1 day"),
    (3, 31, "Mahavir Jayanti", "1 day"),
    (4, 3, "Good Friday ✝️", "1 day"),
    (4, 14, "Ambedkar Jayanti", "1 day"),
    (4, 20, "Eid-ul-Fitr (Ramzan) 🌙", "2-3 days (Drivers on leave)"),
    (5, 12, "Buddha Purnima", "1 day"),
    (6, 27, "Eid-ul-Adha (Bakrid) 🐐", "2-3 days (Drivers on leave)"),
    (7, 17, "Muharram 🏴", "1 day"),
    (8, 15, "Independence Day 🇮🇳", "1 day"),
    (8, 25, "Janmashtami 🪈", "2 days"),
    (8, 27, "Ganesh Chaturthi Starts 🐘", "10 days (Mumbai/Pune slow)"), 
    (9, 7, "Anant Chaturdashi (Ganesh Visarjan)", "1 day (Traffic closed)"),
    (10, 2, "Gandhi Jayanti", "1 day"),
    (10, 10, "Navratri Starts 💃", "9 days (Guj/Bengal busy evenings)"),
    (10, 19, "Dussehra (Vijaya Dashami) 🏹", "1 day"),
    (10, 20, "Dhanteras 💰", "start of Diwali - 5 days"),
    (11, 1, "Diwali (Deepavali) 🪔", "5-7 days (Total Market Closure)"),
    (11, 2, "Govardhan Puja / New Year", "2 days"),
    (11, 3, "Bhai Dooj", "1 day"),
    (11, 15, "Guru Nanak Jayanti 👳", "2 days (Punjab closed)"),
    (12, 25, "Christmas 🎄", "1-5 days (South/Goa festive)"),
]

# ============ STATE-SPECIFIC HOLIDAYS & ATMOSPHERE ============
STATE_HOLIDAYS_ATMOSPHERE = {
    "Gujarat": {
        "holidays": [
            (1, 14, "Uttarayan (Kite Festival) 🪁", "3 days (14-16 Jan) - Total Shutdown"),
            (8, 26, "Janmashtami", "2-3 days - Saurahtra closed"),
            (11, 1, "Diwali (Main)", "7-10 days (Labh Pancham restart)"),
            (11, 5, "Labh Pancham (Business Restart)", "1 day"),
        ],
        "atmosphere": "Diwali: 7-10 days vacation. Uttarayan: 2 days total off."
    },
    "Maharashtra": {
        "holidays": [
            (2, 19, "Chhatrapati Shivaji Maharaj Jayanti", "1 day"),
            (3, 20, "Gudi Padwa (New Year)", "1 day"),
            (5, 1, "Maharashtra Day", "1 day"),
            (8, 27, "Ganesh Chaturthi Starts 🐘", "10 days (Slow movement)"),
            (11, 1, "Diwali", "4-5 days"),
        ],
        "atmosphere": "Ganesh Utsav: 10 days heavy traffic/processions. Diwali: 4 days."
    },
    "Karnataka": {
        "holidays": [
            (1, 14, "Makar Sankranti", "1 day"),
            (3, 20, "Ugadi", "1 day"),
            (11, 1, "Kannada Rajyotsava", "1 day - Flag hoisting/Closed"),
            (10, 19, "Dasara (Mysore)", "3 days - Mysore closed"),
        ],
        "atmosphere": "Dasara: Mysore region very busy. Nov 1st: State Pride day."
    },
    "Tamil Nadu": {
        "holidays": [
            (1, 14, "Pongal (Bogi)", "4 days (14-17 Jan) - Major Festival"),
            (1, 15, "Thiruvalluvar Day", "Pongal Day 2"),
            (1, 16, "Uzhavar Thirunal", "Pongal Day 3"),
            (4, 14, "Tamil New Year (Puthandu)", "1 day"),
            (11, 1, "Deepavali", "3 days"),
        ],
        "atmosphere": "Pongal: 4-5 days (Jan 14-17) - Transport stops. Diwali: 2-3 days."
    },
    "Kerala": {
        "holidays": [
            (4, 14, "Vishu", "1 day"),
            (8, 27, "Onam Starts 🛶", "10 days atmosphere (Main day 27th)"),
            (9, 6, "Thiruvonam", "Main Onam day"),
        ],
        "atmosphere": "Onam: 10 days festive mood. Workers on leave."
    },
    "West Bengal": {
        "holidays": [
            (1, 23, "Netaji Subhas Chandra Bose Jayanti", "1 day"),
            (4, 15, "Bengali New Year", "1 day"),
            (5, 9, "Rabindra Jayanti", "1 day"),
            (10, 19, "Durga Puja (Saptami-Dashami) 🔱", "5-7 days - Total State Shutdown"),
            (10, 23, "Kali Puja", "1 day"),
        ],
        "atmosphere": "Durga Puja: 5-7 days complete closure of business/logistics."
    },
    "Rajasthan": {
        "holidays": [
            (3, 5, "Holi (Dhulandi)", "2-3 days full celebration"),
            (3, 30, "Rajasthan Diwas", "1 day"),
            (11, 1, "Diwali", "5-7 days"),
            (4, 10, "Gangaur", "1 day - Women's festival"),
        ],
        "atmosphere": "Holi: Very big in Rajasthan. Diwali: 7 days."
    },
    "Punjab": {
        "holidays": [
            (1, 13, "Lohri 🔥", "1 day (Evening)"),
            (4, 13, "Baisakhi 🌾", "1 day (Harvest)"),
            (11, 15, "Guru Nanak Jayanti", "1 day"),
        ],
        "atmosphere": "Lohri: 1 day. Diwali: 2-3 days."
    },
    "Bihar": {
        "holidays": [
            (3, 22, "Bihar Diwas", "1 day"),
            (11, 6, "Chhath Puja (Sandhya Arghya) ☀️", "4 days (Huge festival)"),
            (11, 7, "Chhath Puja (Morning)", "4 days"),
        ],
        "atmosphere": "Chhath Puja: 4 days (Nov) - Labor force absent."
    },
     "Uttar Pradesh": {
        "holidays": [
             (3, 5, "Holi", "3-4 days (Lathmar Holi area 1 week)"),
             (11, 1, "Diwali", "5 days"),
        ],
        "atmosphere": "Holi: 3-4 days aggressive celebration. Labor leaves."
    },
     "Delhi": {
        "holidays": [
             (3, 5, "Holi", "2 days"),
             (11, 1, "Diwali", "3-4 days"),
        ],
        "atmosphere": "Diwali: Pollution bans on construction likely."
    }
}

MONTH_NAMES = ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

def get_season_status(city: str, month: int) -> Dict:
    """Returns season status object for a given city and month."""
    city_data = CITY_SEASONS.get(city)
    
    if not city_data:
        return {"status": "unknown", "color": "#9be3e3", "label": "Unknown"}
        
    # Check Peak
    for start, end in city_data.get("peak", []):
        if start <= month <= end:
            return {"status": "peak", "color": "#16A34A", "label": "✅ PEAK SEASON"}
            
    # Check Off
    for start, end in city_data.get("off", []):
        if start <= month <= end:
            return {"status": "off", "color": "#DC2626", "label": "❌ OFF SEASON"}
            
    # Check Moderate
    return {"status": "moderate", "color": "#D97706", "label": "⚠️ MODERATE"}

def get_holidays_for_month(year: int, month: int, state: str = None) -> List[Dict]:
    """Returns combined list of holidays for the month."""
    holidays = []
    
    # National & Major
    for m, d, name, dur in MAJOR_FESTIVALS_2026:
        if m == month:
            holidays.append({"date": datetime.date(year, m, d), "name": name, "type": "national", "duration": dur})
            
    # State Specific
    if state and state in STATE_HOLIDAYS_ATMOSPHERE:
        for m, d, name, dur in STATE_HOLIDAYS_ATMOSPHERE[state]["holidays"]:
            if m == month:
                holidays.append({"date": datetime.date(year, m, d), "name": name, "type": "state", "duration": dur})
                
    return sorted(holidays, key=lambda x: x['date'])

def get_city_remarks(city: str) -> str:
    if city in CITY_SEASONS:
        return CITY_SEASONS[city].get("remarks", "")
    return ""

def get_state_atmosphere(state: str) -> str:
    """Returns general atmospheric remarks for the state."""
    if state in STATE_HOLIDAYS_ATMOSPHERE:
        return STATE_HOLIDAYS_ATMOSPHERE[state].get("atmosphere", "")
    return ""

def get_sundays(year: int, month: int) -> List[int]:
    """Returns a list of days (int) that are Sundays in the given month."""
    sundays = []
    # calendar.monthcalendar returns list of weeks
    for week in calendar.monthcalendar(year, month):
        # week is [Mon, Tue, Wed, Thu, Fri, Sat, Sun]
        # Sun is index 6. If 0, it belongs to other month.
        if week[6] != 0:
            sundays.append(week[6])
    return sundays

def get_yearly_overview(city: str) -> List[Dict]:
    """Returns a list of 12 month season status objects."""
    overview = []
    for m in range(1, 13):
        status = get_season_status(city, m)
        status['month_name'] = MONTH_NAMES[m]
        overview.append(status)
    return overview

STATE_HOLIDAYS = STATE_HOLIDAYS_ATMOSPHERE
