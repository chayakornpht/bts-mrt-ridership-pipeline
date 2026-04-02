"""
Extract MRT ridership data from BEM Investor Relations
Source: https://investor.bemplc.co.th/en/ridership-report/ridership
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import psycopg2
from datetime import datetime
import os
import json
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Database connection
DB_CONFIG = {
    'host': os.environ.get('WAREHOUSE_HOST', 'localhost'),
    'port': os.environ.get('WAREHOUSE_PORT', '5434'),
    'dbname': os.environ.get('WAREHOUSE_DB', 'bts_mrt'),
    'user': os.environ.get('WAREHOUSE_USER', 'warehouse'),
    'password': os.environ.get('WAREHOUSE_PASSWORD', 'warehouse123'),
}

BEM_RIDERSHIP_URL = "https://investor.bemplc.co.th/en/ridership-report/ridership"


def fetch_bem_page():
    """Fetch BEM ridership page HTML."""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Educational Project - BTS/MRT Ridership Analysis)'
    }
    try:
        response = requests.get(BEM_RIDERSHIP_URL, headers=headers, timeout=30)
        response.raise_for_status()
        logger.info(f"Successfully fetched BEM page: {response.status_code}")
        return response.text
    except requests.RequestException as e:
        logger.error(f"Failed to fetch BEM page: {e}")
        raise


def parse_ridership_table(html_content):
    """
    Parse ridership data from BEM's investor page.
    
    NOTE: BEM's website structure may change over time.
    This parser extracts tables containing ridership data.
    You may need to adjust the parsing logic based on the current HTML structure.
    
    Returns a list of dicts with keys:
        - report_month (YYYY-MM-01)
        - line_name (MRT Blue Line / MRT Purple Line)
        - daily_avg_passengers
        - monthly_total_passengers
    """
    soup = BeautifulSoup(html_content, 'html.parser')
    records = []
    
    # Look for tables on the page
    tables = soup.find_all('table')
    
    if not tables:
        logger.warning("No tables found on BEM page. Trying alternative parsing...")
        # Try finding data in div/span elements (BEM sometimes uses non-table layout)
        return parse_alternative_layout(soup)
    
    for table in tables:
        rows = table.find_all('tr')
        headers = []
        
        for row in rows:
            cells = row.find_all(['th', 'td'])
            cell_texts = [cell.get_text(strip=True) for cell in cells]
            
            if not headers:
                headers = cell_texts
                continue
            
            if len(cell_texts) < 2:
                continue
            
            # Try to parse each row as ridership data
            record = parse_ridership_row(cell_texts, headers)
            if record:
                records.append(record)
    
    logger.info(f"Parsed {len(records)} ridership records from BEM")
    return records


def parse_alternative_layout(soup):
    """
    Fallback parser if BEM doesn't use standard tables.
    Looks for common patterns in investor relations pages.
    """
    records = []
    
    # Look for year/month headers with ridership numbers
    text_blocks = soup.find_all(['div', 'span', 'p'])
    
    for block in text_blocks:
        text = block.get_text(strip=True)
        # Look for patterns like "January 2024: 450,000"
        # This is a simplified example - adjust based on actual page structure
        if any(month in text.lower() for month in ['january', 'february', 'march']):
            logger.debug(f"Found potential ridership text: {text[:100]}")
    
    return records


def parse_ridership_row(cells, headers):
    """Parse a single row of ridership data."""
    try:
        record = {}
        
        for i, header in enumerate(headers):
            if i >= len(cells):
                break
            
            header_lower = header.lower()
            value = cells[i].replace(',', '').strip()
            
            if 'month' in header_lower or 'period' in header_lower:
                record['period'] = cells[i]
            elif 'blue' in header_lower:
                record['blue_line'] = safe_float(value)
            elif 'purple' in header_lower:
                record['purple_line'] = safe_float(value)
            elif 'total' in header_lower:
                record['total'] = safe_float(value)
        
        if 'period' in record:
            return record
        return None
        
    except Exception as e:
        logger.debug(f"Could not parse row: {e}")
        return None


def safe_float(value):
    """Safely convert string to float."""
    try:
        return float(value.replace(',', '').replace(' ', ''))
    except (ValueError, AttributeError):
        return None


def load_to_warehouse(records):
    """Load parsed records into the raw schema."""
    if not records:
        logger.warning("No records to load")
        return 0
    
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    loaded = 0
    for record in records:
        try:
            # Insert Blue Line data
            if record.get('blue_line'):
                cur.execute("""
                    INSERT INTO raw.bem_ridership 
                    (report_month, line_name, daily_avg_passengers, extracted_at)
                    VALUES (%s, %s, %s, NOW())
                    ON CONFLICT DO NOTHING
                """, (
                    record.get('period', 'unknown'),
                    'MRT Blue Line',
                    record['blue_line']
                ))
                loaded += 1
            
            # Insert Purple Line data
            if record.get('purple_line'):
                cur.execute("""
                    INSERT INTO raw.bem_ridership 
                    (report_month, line_name, daily_avg_passengers, extracted_at)
                    VALUES (%s, %s, %s, NOW())
                    ON CONFLICT DO NOTHING
                """, (
                    record.get('period', 'unknown'),
                    'MRT Purple Line',
                    record['purple_line']
                ))
                loaded += 1
                
        except Exception as e:
            logger.error(f"Failed to insert record: {e}")
            conn.rollback()
            continue
    
    conn.commit()
    cur.close()
    conn.close()
    
    logger.info(f"Loaded {loaded} records into raw.bem_ridership")
    return loaded


def extract_bem_ridership():
    """Main extraction function - called by Airflow."""
    logger.info("Starting BEM ridership extraction...")
    
    html_content = fetch_bem_page()
    records = parse_ridership_table(html_content)
    
    if records:
        loaded = load_to_warehouse(records)
        logger.info(f"BEM extraction complete: {loaded} records loaded")
    else:
        logger.warning("No records parsed - saving raw HTML for manual inspection")
        os.makedirs('/opt/airflow/data/raw', exist_ok=True)
        with open(f'/opt/airflow/data/raw/bem_page_{datetime.now().strftime("%Y%m%d")}.html', 'w') as f:
            f.write(html_content)
        logger.info("Raw HTML saved for debugging")
    
    return len(records)


if __name__ == '__main__':
    extract_bem_ridership()
