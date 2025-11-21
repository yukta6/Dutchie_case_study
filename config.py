"""
Configuration file for Dutchie POS Dashboard
"""

API_KEYS = {
    "Columbus": "eb0a049ab2f34161b9cd79beedd20d5d",
    "Cincinnati": "8e1409cfe96d48f98cc89820c10fa36d"
}

LOCATIONS = {
    "Columbus": {
        "id": "loc_001",
        "name": "Columbus",
        "timezone": "America/New_York",
        "api_key": API_KEYS["Columbus"]
    },
    "Cincinnati": {
        "id": "loc_002", 
        "name": "Cincinnati",
        "timezone": "America/New_York",
        "api_key": API_KEYS["Cincinnati"]
    }
}

DEFAULT_TIMEZONE = "America/New_York"

def get_location_config(location_name):
    """Get location configuration with fallback for uploaded locations"""
    if location_name in LOCATIONS:
        return LOCATIONS[location_name]
    else:
        return {
            "id": f"loc_{hash(location_name) % 10000:04d}",
            "name": location_name,
            "timezone": DEFAULT_TIMEZONE,
            "api_key": None
        }


def register_uploaded_location(location_name, timezone=None):
    """Register a new uploaded location in the LOCATIONS dict"""
    if location_name not in LOCATIONS:
        LOCATIONS[location_name] = {
            "id": f"loc_{hash(location_name) % 10000:04d}",
            "name": location_name,
            "timezone": timezone or DEFAULT_TIMEZONE,
            "api_key": None
        }

STORE_HOURS = {
    "open": 9,
    "close": 21
}

DAYPARTS = {
    "Morning": (9, 12),
    "Afternoon": (12, 17),
    "Evening": (17, 21)
}

THRESHOLDS = {
    "void_spike_multiplier": 2.0,
    "refund_spike_multiplier": 2.0,
    "discount_rate_high": 30.0,
    "negative_total_threshold": -0.01
}

DB_PATH = "dutchie_pos.db"

API_BASE_URL = "https://api.pos.dutchie.com"

MOCK_DATA_CONFIG = {
    "days_of_data": 56,  # 8 weeks of data for period-over-period comparisons
    "transactions_per_day": (50, 150),
    "products_count": 50,
    "staff_count": 8
}
