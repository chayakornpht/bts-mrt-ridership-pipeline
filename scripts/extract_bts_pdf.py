"""
Extract BTS ridership data from Annual Report PDFs
Source: https://www.bts.co.th/eng/library/system-annual.html

Usage:
    1. Download PDF manually from BTS website
    2. Place in /data/pdfs/ directory
    3. Run this script
"""

import pdfplumber
import pandas as pd
import psycopg2
import os
import re
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DB_CONFIG = {
    'host': os.environ.get('WAREHOUSE_HOST', 'localhost'),
    'port': os.environ.get('WAREHOUSE_PORT', '5434'),
    'dbname': os.environ.get('WAREHOUSE_DB', 'bts_mrt'),
    'user': os.environ.get('WAREHOUSE_USER', 'warehouse'),
    'password': os.environ.get('WAREHOUSE_PASSWORD', 'warehouse123'),
}

PDF_DIR = os.environ.get('PDF_DIR', '/opt/airflow/data/pdfs')


def find_pdf_files():
    """Find all BTS annual report PDFs in the data directory."""
    pdf_dir = Path(PDF_DIR)
    pdf_dir.mkdir(parents=True, exist_ok=True)
    
    pdfs = list(pdf_dir.glob('*.pdf'))
    logger.info(f"Found {len(pdfs)} PDF files in {PDF_DIR}")
    return pdfs


def extract_tables_from_pdf(pdf_path):
    """
    Extract ridership tables from a BTS annual report PDF.
    
    BTS annual reports typically contain:
    - Total ridership by line (Sukhumvit / Silom)
    - Average daily ridership
    - Revenue data
    - Quarterly breakdown
    
    NOTE: PDF structure varies by year. This script handles common patterns
    but may need adjustment for specific annual reports.
    """
    records = []
    
    try:
        with pdfplumber.open(pdf_path) as pdf:
            logger.info(f"Processing {pdf_path.name}: {len(pdf.pages)} pages")
            
            for page_num, page in enumerate(pdf.pages):
                # Extract tables from each page
                tables = page.extract_tables()
                
                if not tables:
                    continue
                
                for table in tables:
                    page_records = parse_ridership_table(table, pdf_path.name)
                    records.extend(page_records)
                
                # Also try extracting text for non-tabular data
                text = page.extract_text()
                if text:
                    text_records = parse_ridership_text(text, pdf_path.name)
                    records.extend(text_records)
    
    except Exception as e:
        logger.error(f"Failed to process PDF {pdf_path}: {e}")
    
    logger.info(f"Extracted {len(records)} records from {pdf_path.name}")
    return records


def parse_ridership_table(table, source_file):
    """
    Parse a table extracted from PDF.
    
    Common patterns in BTS reports:
    - Headers: Year/Quarter, Sukhumvit Line, Silom Line, Total
    - Values: passenger counts (millions) and revenue (THB millions)
    """
    records = []
    
    if not table or len(table) < 2:
        return records
    
    # Try to identify header row
    headers = [str(cell).lower().strip() if cell else '' for cell in table[0]]
    
    # Check if this looks like a ridership table
    ridership_keywords = ['passenger', 'ridership', 'trip', 'ผู้โดยสาร', 'เที่ยว']
    line_keywords = ['sukhumvit', 'silom', 'สุขุมวิท', 'สีลม', 'green', 'total']
    
    is_ridership = any(kw in ' '.join(headers) for kw in ridership_keywords)
    has_lines = any(kw in ' '.join(headers) for kw in line_keywords)
    
    if not (is_ridership or has_lines):
        return records
    
    logger.info(f"Found potential ridership table with headers: {headers}")
    
    for row in table[1:]:
        if not row or all(cell is None for cell in row):
            continue
        
        cells = [str(cell).strip() if cell else '' for cell in row]
        
        # Try to extract fiscal year
        fiscal_year = extract_year(cells[0]) if cells else None
        
        if fiscal_year is None:
            continue
        
        # Try to find numeric values for each line
        for i, header in enumerate(headers):
            if i >= len(cells):
                break
            
            value = extract_number(cells[i])
            if value is None:
                continue
            
            line_name = identify_line(header)
            if line_name:
                records.append({
                    'fiscal_year': fiscal_year,
                    'line_name': line_name,
                    'value': value,
                    'source_file': source_file,
                })
    
    return records


def parse_ridership_text(text, source_file):
    """
    Parse ridership data from free text in PDF.
    
    Looks for patterns like:
    - "total ridership of 200.5 million"
    - "average daily ridership of 750,000"
    - "Sukhumvit Line: 120.3 million trips"
    """
    records = []
    
    # Pattern: number followed by "million" near "ridership" or "passenger"
    patterns = [
        r'(?:total|overall)\s+(?:ridership|passengers?)\s+(?:of\s+)?(\d+[\d,.]*)\s*(?:million)',
        r'(?:sukhumvit|silom|green)\s+line[:\s]+(\d+[\d,.]*)\s*(?:million)',
        r'(?:average\s+daily)\s+(?:ridership|passengers?)\s+(?:of\s+)?(\d+[\d,.]*)',
    ]
    
    for pattern in patterns:
        matches = re.finditer(pattern, text.lower())
        for match in matches:
            try:
                value = float(match.group(1).replace(',', ''))
                records.append({
                    'text_match': match.group(0),
                    'value': value,
                    'source_file': source_file,
                })
            except ValueError:
                continue
    
    return records


def extract_year(text):
    """Extract a 4-digit year from text."""
    match = re.search(r'20[12]\d', str(text))
    if match:
        return int(match.group())
    
    # Thai Buddhist Era year (e.g., 2566 = 2023 CE)
    match = re.search(r'25[5-7]\d', str(text))
    if match:
        return int(match.group()) - 543
    
    return None


def extract_number(text):
    """Extract a numeric value from text."""
    if not text:
        return None
    
    # Remove common formatting
    cleaned = re.sub(r'[,\s]', '', str(text))
    
    try:
        value = float(cleaned)
        if value > 0:
            return value
    except ValueError:
        pass
    
    return None


def identify_line(header):
    """Identify which BTS line a column header refers to."""
    header_lower = str(header).lower()
    
    if any(kw in header_lower for kw in ['sukhumvit', 'สุขุมวิท']):
        return 'BTS Sukhumvit Line'
    elif any(kw in header_lower for kw in ['silom', 'สีลม']):
        return 'BTS Silom Line'
    elif any(kw in header_lower for kw in ['total', 'รวม']):
        return 'BTS Total'
    elif any(kw in header_lower for kw in ['gold', 'ทอง']):
        return 'BTS Gold Line'
    
    return None


def load_to_warehouse(records):
    """Load extracted records to raw schema."""
    if not records:
        logger.warning("No records to load")
        return 0
    
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    loaded = 0
    for record in records:
        try:
            if 'fiscal_year' in record and 'line_name' in record:
                cur.execute("""
                    INSERT INTO raw.bts_ridership 
                    (fiscal_year, line_name, total_passengers, source_file, extracted_at)
                    VALUES (%s, %s, %s, %s, NOW())
                """, (
                    record['fiscal_year'],
                    record['line_name'],
                    record['value'],
                    record.get('source_file', 'unknown'),
                ))
                loaded += 1
                
        except Exception as e:
            logger.error(f"Failed to insert: {e}")
            conn.rollback()
            continue
    
    conn.commit()
    cur.close()
    conn.close()
    
    logger.info(f"Loaded {loaded} records into raw.bts_ridership")
    return loaded


def extract_bts_pdfs():
    """Main extraction function - called by Airflow."""
    logger.info("Starting BTS PDF extraction...")
    
    pdf_files = find_pdf_files()
    
    if not pdf_files:
        logger.warning(f"No PDF files found in {PDF_DIR}")
        logger.info("Download BTS annual reports from: https://www.bts.co.th/eng/library/system-annual.html")
        return 0
    
    all_records = []
    for pdf_path in pdf_files:
        records = extract_tables_from_pdf(pdf_path)
        all_records.extend(records)
    
    loaded = load_to_warehouse(all_records)
    logger.info(f"BTS extraction complete: {loaded} records from {len(pdf_files)} PDFs")
    return loaded


if __name__ == '__main__':
    extract_bts_pdfs()
