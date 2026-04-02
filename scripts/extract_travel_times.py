"""
Extract travel time data between BTS/MRT stations using Google Maps Distance Matrix API.

Setup:
    1. Get API key from https://console.cloud.google.com
    2. Enable Distance Matrix API
    3. Set env var: export GOOGLE_MAPS_API_KEY=your_key_here
    
Free tier: $200/month credit (enough for ~40,000 requests)
"""

import requests
import psycopg2
import os
import json
import time
import logging
from datetime import datetime, timedelta
from itertools import combinations

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DB_CONFIG = {
    'host': os.environ.get('WAREHOUSE_HOST', 'localhost'),
    'port': os.environ.get('WAREHOUSE_PORT', '5434'),
    'dbname': os.environ.get('WAREHOUSE_DB', 'bts_mrt'),
    'user': os.environ.get('WAREHOUSE_USER', 'warehouse'),
    'password': os.environ.get('WAREHOUSE_PASSWORD', 'warehouse123'),
}

API_KEY = os.environ.get('GOOGLE_MAPS_API_KEY', '')
DISTANCE_MATRIX_URL = "https://maps.googleapis.com/maps/api/distancematrix/json"

# Time slots to capture (Bangkok timezone UTC+7)
TIME_SLOTS = {
    'morning_rush': '07:30',
    'midday': '12:00',
    'evening_rush': '18:00',
    'late_night': '22:00',
}


def get_station_pairs():
    """
    Get station coordinates from warehouse for major interchange stations.
    We measure travel times between key interchange points, not every station pair.
    """
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    cur.execute("""
        SELECT station_code, name_en, latitude, longitude 
        FROM raw.stations 
        WHERE is_interchange = TRUE 
        AND latitude IS NOT NULL
        ORDER BY station_code
    """)
    
    stations = cur.fetchall()
    cur.close()
    conn.close()
    
    logger.info(f"Found {len(stations)} interchange stations for travel time analysis")
    return stations


def fetch_travel_time(origin_lat, origin_lng, dest_lat, dest_lng, departure_time=None):
    """
    Call Google Maps Distance Matrix API for a single origin-destination pair.
    Uses transit mode to get public transport travel times.
    """
    if not API_KEY or API_KEY == 'your_api_key_here':
        logger.warning("Google Maps API key not set. Using mock data for development.")
        return generate_mock_travel_time()
    
    params = {
        'origins': f'{origin_lat},{origin_lng}',
        'destinations': f'{dest_lat},{dest_lng}',
        'mode': 'transit',
        'transit_mode': 'rail',
        'key': API_KEY,
    }
    
    if departure_time:
        params['departure_time'] = int(departure_time.timestamp())
    
    try:
        response = requests.get(DISTANCE_MATRIX_URL, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        if data['status'] == 'OK':
            element = data['rows'][0]['elements'][0]
            if element['status'] == 'OK':
                return {
                    'duration_seconds': element['duration']['value'],
                    'distance_meters': element.get('distance', {}).get('value'),
                }
        
        logger.warning(f"API returned status: {data.get('status')}")
        return None
        
    except requests.RequestException as e:
        logger.error(f"API request failed: {e}")
        return None


def generate_mock_travel_time():
    """Generate realistic mock data for development without API key."""
    import random
    return {
        'duration_seconds': random.randint(600, 3600),  # 10-60 minutes
        'distance_meters': random.randint(2000, 25000),  # 2-25 km
    }


def extract_travel_times(time_slot_name=None):
    """
    Main extraction function.
    
    Args:
        time_slot_name: If specified, only extract for this time slot.
                       Otherwise extract for all time slots.
    """
    logger.info("Starting travel time extraction...")
    
    stations = get_station_pairs()
    if len(stations) < 2:
        logger.warning("Not enough stations to calculate travel times")
        return 0
    
    # Generate station pairs (not all combinations - just key routes)
    pairs = list(combinations(stations, 2))
    logger.info(f"Will extract travel times for {len(pairs)} station pairs")
    
    slots = {time_slot_name: TIME_SLOTS[time_slot_name]} if time_slot_name else TIME_SLOTS
    
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    today = datetime.now().date()
    loaded = 0
    
    for slot_name, slot_time in slots.items():
        logger.info(f"Extracting for time slot: {slot_name} ({slot_time})")
        
        for origin, destination in pairs:
            origin_code, origin_name, origin_lat, origin_lng = origin
            dest_code, dest_name, dest_lat, dest_lng = destination
            
            result = fetch_travel_time(origin_lat, origin_lng, dest_lat, dest_lng)
            
            if result:
                cur.execute("""
                    INSERT INTO raw.travel_times 
                    (from_station_code, to_station_code, time_slot, 
                     duration_seconds, distance_meters, captured_date, captured_at)
                    VALUES (%s, %s, %s, %s, %s, %s, NOW())
                """, (
                    origin_code, dest_code, slot_name,
                    result['duration_seconds'], result['distance_meters'],
                    today,
                ))
                loaded += 1
            
            # Rate limiting: max 50 requests per second for Distance Matrix
            time.sleep(0.1)
        
        conn.commit()
    
    cur.close()
    conn.close()
    
    logger.info(f"Travel time extraction complete: {loaded} records loaded")
    return loaded


if __name__ == '__main__':
    extract_travel_times()
