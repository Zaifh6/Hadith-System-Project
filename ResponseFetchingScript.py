import requests
import json
import csv
import os
import re
import time
import random
import logging
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse, parse_qs

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Create logs directory
log_dir = Path("logs")
log_dir.mkdir(exist_ok=True)

# Create output directory for JSON files
output_dir = Path("scraped_hadith_json")
output_dir.mkdir(exist_ok=True)

# Setup log files
success_log = log_dir / f"success_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
error_log = log_dir / f"error_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"

# Rotating User Agents
user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:128.0) Gecko/20100101 Firefox/128.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36 Edg/134.0.0.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Mobile/15E148 Safari/604.1"
]

# API Endpoints
HADITH_DETAILS_ENDPOINT = "https://hadith.inoor.ir/service/api/elastic/ElasticHadithById"
HADITH_REJAL_ENDPOINT = "https://hadith.inoor.ir/service/api/hadith/HadithRejalList/v2"

# Path to the sitemap file - adjust this to your actual sitemap file path
SITEMAP_PATH = "sitemap.xml"  # Change this to your actual sitemap path

def get_random_delay():
    """Generate a random delay between requests to avoid detection"""
    return random.uniform(1.5, 5.0)

def get_headers():
    """Get headers with a random user agent"""
    return {
        "accept": "application/json, text/plain, */*",
        "content-type": "application/json",
        "referer": "https://hadith.inoor.ir/",
        "user-agent": random.choice(user_agents)
    }

def extract_hadith_ids_from_sitemap(sitemap_path):
    """Extract hadith IDs from the sitemap file"""
    try:
        if not os.path.exists(sitemap_path):
            logging.error(f"Sitemap file not found: {sitemap_path}")
            return []
        
        hadith_ids = []
        tree = ET.parse(sitemap_path)
        root = tree.getroot()
        
        # Define namespace if needed - adjust according to your sitemap format
        namespaces = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
        
        # Extract URLs and parse for hadith IDs
        for url in root.findall(".//ns:url/ns:loc", namespaces) or root.findall(".//url/loc"):
            url_text = url.text
            if 'hadith.inoor.ir' in url_text and '/h/' in url_text:
                # Extract hadith ID from URL - adjust pattern as needed
                match = re.search(r'/h/(\d+)', url_text)
                if match:
                    hadith_id = match.group(1)
                    hadith_ids.append(hadith_id)
        
        logging.info(f"Extracted {len(hadith_ids)} hadith IDs from sitemap")
        return hadith_ids
    except Exception as e:
        logging.error(f"Error extracting hadith IDs from sitemap: {e}")
        return []

def fetch_hadith_data(hadith_id):
    """Fetch hadith details and rejal data for a given hadith ID"""
    try:
        # Get hadith details
        payload = {
            "hadithId": [hadith_id],
            "searchPhrase": ""
        }
        
        headers = get_headers()
        response = requests.post(
            HADITH_DETAILS_ENDPOINT, 
            json=payload, 
            headers=headers,
            timeout=30
        )
        
        if response.status_code != 200:
            return None, f"Failed to fetch hadith details: HTTP {response.status_code}"
        
        hadith_details = response.json()
        
        # Add delay between requests
        time.sleep(get_random_delay())
        
        # Get rejal data
        rejal_url = f"{HADITH_REJAL_ENDPOINT}?hadithId={hadith_id}"
        headers = get_headers()  # Get a possibly different user agent
        rejal_response = requests.get(
            rejal_url, 
            headers=headers,
            timeout=30
        )
        
        if rejal_response.status_code != 200:
            return None, f"Failed to fetch rejal data: HTTP {rejal_response.status_code}"
        
        rejal_data = rejal_response.json()
        
        # Combine both responses
        final_data = {
            "hadith_id": hadith_id,
            "hadith_details": hadith_details,
            "hadith_rejal_list": rejal_data
        }
        
        return final_data, None
    except requests.exceptions.Timeout:
        return None, f"Request timed out for hadith ID {hadith_id}"
    except requests.exceptions.ConnectionError:
        return None, f"Connection error for hadith ID {hadith_id}"
    except Exception as e:
        return None, f"Error fetching data for hadith ID {hadith_id}: {str(e)}"

def save_to_csv(hadith_data, csv_file):
    """Save processed hadith data to CSV file"""
    try:
        # CSV headers
        csv_headers = ['hadith_id', 'content', 'originated_from', 'book_id', 'title', 'volume', 'page_number']
        
        # Create CSV file if it doesn't exist
        file_exists = os.path.exists(csv_file)
        
        with open(csv_file, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            
            if not file_exists:
                writer.writerow(csv_headers)
            
            hadith = hadith_data['hadith_details']['data'][0]
            
            # Find the matching hadith in groupTogetherList
            hadith_info = None
            hadith_id = hadith_data['hadith_id']
            
            for group in hadith.get('groupTogetherList', []):
                if str(group.get('hadithId')) == hadith_id:
                    hadith_info = group
                    break
            
            if hadith_info:
                # Get and clean content
                content = hadith.get('textSample', '')
                clean_content = content
                
                for tag in ['Hadith', 'Document', 'Narrator', 'Exporter', 'Innocent']:
                    clean_content = clean_content.replace(f'<{tag}>', '').replace(f'</{tag}>', '')
                
                # Extract required data
                csv_row = [
                    hadith_id,
                    clean_content.strip(),
                    hadith.get('qaelTitleList', ''),
                    hadith_info.get('sourceId', ''),
                    hadith_info.get('sourceMainTitle', ''),
                    hadith_info.get('vol', ''),
                    hadith_info.get('pageNum', '')
                ]
                
                writer.writerow(csv_row)
                return True
        
        return False
    except Exception as e:
        logging.error(f"Error saving to CSV: {e}")
        return False

def process_hadith_ids(hadith_ids, csv_file, max_retries=3):
    """Process a list of hadith IDs, with retry logic"""
    total = len(hadith_ids)
    success_count = 0
    error_count = 0
    
    with open(success_log, 'w', encoding='utf-8') as s_log, \
         open(error_log, 'w', encoding='utf-8') as e_log:
        
        for index, hadith_id in enumerate(hadith_ids):
            logging.info(f"Processing {index+1}/{total}: Hadith ID {hadith_id}")
            
            retries = 0
            success = False
            
            while retries < max_retries and not success:
                if retries > 0:
                    # Exponential backoff
                    wait_time = 2 ** retries + random.uniform(0, 1)
                    logging.info(f"Retry {retries}/{max_retries} for hadith ID {hadith_id}. Waiting {wait_time:.2f} seconds...")
                    time.sleep(wait_time)
                
                data, error = fetch_hadith_data(hadith_id)
                
                if data:
                    # Save JSON response
                    json_path = output_dir / f"hadith_{hadith_id}.json"
                    with open(json_path, "w", encoding="utf-8") as file:
                        json.dump(data, file, indent=4, ensure_ascii=False)
                    
                    # Save to CSV
                    csv_success = save_to_csv(data, csv_file)
                    
                    if csv_success:
                        success = True
                        success_count += 1
                        s_log.write(f"{hadith_id}\n")
                        logging.info(f"Successfully processed hadith ID {hadith_id}")
                    else:
                        error = "Failed to save to CSV"
                
                retries += 1
            
            # Add to error log if all retries failed
            if not success:
                error_count += 1
                e_log.write(f"{hadith_id}: {error}\n")
                logging.error(f"Failed to process hadith ID {hadith_id}: {error}")
            
            # Add a random delay between processing different hadiths
            if index < total - 1:  # Don't delay after the last item
                time.sleep(get_random_delay())
    
    return success_count, error_count

def main():
    # CSV file for structured data
    csv_file = 'hadith_data.csv'
    
    # Get hadith IDs from sitemap
    print("Extracting hadith IDs from sitemap...")
    hadith_ids = extract_hadith_ids_from_sitemap(SITEMAP_PATH)
    
    if not hadith_ids:
        print("No hadith IDs found. Please check the sitemap path.")
        return
    
    # Process all hadith IDs
    print(f"Starting to process {len(hadith_ids)} hadith IDs...")
    success_count, error_count = process_hadith_ids(hadith_ids, csv_file)
    
    # Final report
    print("\n--- Scraping Complete ---")
    print(f"Total hadith IDs processed: {len(hadith_ids)}")
    print(f"Successfully processed: {success_count}")
    print(f"Failed to process: {error_count}")
    print(f"Success log: {success_log}")
    print(f"Error log: {error_log}")
    print(f"JSON files saved to: {output_dir}")
    print(f"CSV data saved to: {csv_file}")

if __name__ == "__main__":
    main()
