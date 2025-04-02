import csv
import uuid
import re
import os
import requests
import time
import random
import logging
import sys
from datetime import datetime

# Create a log file name with timestamp
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_file_path = f"hadith_processing_log_{timestamp}.txt"

# Set up a file to capture all output
class Logger:
    def __init__(self, filename):
        self.terminal = sys.stdout
        self.log = open(filename, 'w', encoding='utf-8')
        
    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)
        
    def flush(self):
        self.terminal.flush()
        self.log.flush()

# Redirect stdout to both console and file
sys.stdout = Logger(log_file_path)
sys.stderr = Logger(log_file_path)

# Now all print statements and errors will be saved to the file

# Configure logging
logging.basicConfig(
    filename="hadith_processing.log", 
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# File containing Hadith IDs
sitemap_file = "sitemap_1_urls.txt"
csv_folder = "X:\hadith_scraping_script\hadith_scraping_script\csv-data-of-json"

# Ensure the CSV folder exists
os.makedirs(csv_folder, exist_ok=True)

# CSV File Paths
hadith_file = os.path.join(csv_folder, "hadith.csv")
book_file = os.path.join(csv_folder, "book.csv")
reference_file = os.path.join(csv_folder, "reference.csv")
sanad_file = os.path.join(csv_folder, "hadith_sanad.csv")
narrator_file = os.path.join(csv_folder, "narrators.csv")
narrator_chain_file = os.path.join(csv_folder, "hadith_narrator_chain.csv")

# New CSV File Paths for additional tables
narrator_details_file = os.path.join(csv_folder, "narrator_details.csv")
narrator_death_records_file = os.path.join(csv_folder, "narrator_death_records.csv")
narrator_evaluation_file = os.path.join(csv_folder, "narrator_evaluation.csv")

# Print file paths for debugging
print(f"CSV files will be saved to: {csv_folder}")
for file_path in [hadith_file, book_file, reference_file, sanad_file, narrator_file, narrator_chain_file, 
                 narrator_details_file, narrator_death_records_file, narrator_evaluation_file]:
    print(f"File path: {file_path}")
    print(f"  - Directory exists: {os.path.exists(os.path.dirname(file_path))}")
    print(f"  - File exists: {os.path.exists(file_path)}")

# API Endpoints
def get_endpoints(hadith_id):
    return {
        "hadith_details": "https://hadith.inoor.ir/service/api/elastic/ElasticHadithById",
        "hadith_rejal": f"https://hadith.inoor.ir/service/api/hadith/HadithRejalList/v2?hadithId={hadith_id}"
    }

# Function to fetch Hadith details (POST request)
def fetch_hadith_details(hadith_id):
    payload = {"hadithId": [hadith_id], "searchPhrase": ""}
    headers = {"accept": "application/json", "content-type": "application/json"}
    
    try:
        print(f"Fetching hadith details for ID: {hadith_id}")
        response = requests.post(get_endpoints(hadith_id)["hadith_details"], json=payload, headers=headers)
        print(f"Response status code: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            print(f"Response data keys: {list(data.keys()) if isinstance(data, dict) else 'Not a dictionary'}")
            return data
        else:
            logging.error(f"Failed to fetch hadith details for ID {hadith_id}. Status code: {response.status_code}")
            print(f"Error response: {response.text[:100]}...")  # Print first 100 chars
            return {"error": response.text}
    except Exception as e:
        logging.error(f"Exception while fetching hadith details for ID {hadith_id}: {str(e)}")
        return {"error": str(e)}

# Function to fetch reference details for a hadith ID
def fetch_reference_details(reference_hadith_id):
    payload = {"hadithId": [reference_hadith_id], "searchPhrase": ""}
    headers = {"accept": "application/json", "content-type": "application/json"}
    
    try:
        print(f"Fetching reference details for ID: {reference_hadith_id}")
        response = requests.post(get_endpoints(reference_hadith_id)["hadith_details"], json=payload, headers=headers)
        
        if response.status_code == 200:
            data = response.json()
            if "data" in data and data["data"]:
                ref_data = data["data"][0]
                return {
                    "hadith_id": ref_data.get("id", "N/A"),
                    "vol": ref_data.get("vol", "N/A"),
                    "pageNum": ref_data.get("pageNum", "N/A"),
                    "sourceId": ref_data.get("sourceId", "N/A"),
                    "sourceMainTitle": ref_data.get("bookTitle", "Unknown Source")
                }
            return {}
        else:
            logging.error(f"Failed to fetch reference details for ID {reference_hadith_id}. Status code: {response.status_code}")
            return {}
    except Exception as e:
        logging.error(f"Exception while fetching reference details for ID {reference_hadith_id}: {str(e)}")
        return {}

# Function to fetch Hadith Rejal list (GET request)
def fetch_hadith_rejal(hadith_id):
    headers = {"accept": "application/json"}
    
    try:
        print(f"Fetching hadith rejal for ID: {hadith_id}")
        response = requests.get(get_endpoints(hadith_id)["hadith_rejal"], headers=headers)
        print(f"Response status code: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            print(f"Response data keys: {list(data.keys()) if isinstance(data, dict) else 'Not a dictionary'}")
            return data
        else:
            logging.error(f"Failed to fetch hadith rejal for ID {hadith_id}. Status code: {response.status_code}")
            return None
    except Exception as e:
        logging.error(f"Exception while fetching hadith rejal for ID {hadith_id}: {str(e)}")
        return None

# Extract Hadith IDs from the file
def extract_hadith_ids(filename):
    try:
        if not os.path.exists(filename):
            print(f"ERROR: Sitemap file does not exist: {filename}")
            return []
            
        with open(filename, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        print(f"Read {len(lines)} lines from sitemap file")
        
        ids = []
        for line in lines:
            match = re.search(r'/hadith/(\d+)/', line)
            if match:
                ids.append(match.group(1))
        
        print(f"Found {len(ids)} hadith IDs in the sitemap file")
        if ids:
            print(f"First few IDs: {ids[:5]}")
        return ids
    except Exception as e:
        logging.error(f"Error extracting hadith IDs from {filename}: {str(e)}")
        print(f"Exception reading sitemap: {str(e)}")
        return []

# Check if CSV files exist and create headers if needed
def initialize_csv_files():
    files_and_headers = {
        hadith_file: ["uuid", "hadith_id", "content", "originated_from", "book_id"],
        book_file: ["id", "title", "page_num", "volume"],
        reference_file: ["id", "hadith_uuid_fk", "hadith_id", "volume", "page_num", "source_id", "source_title"],
        sanad_file: ["id", "hadith_uuid_fk", "sanad_description", "sanad_number"],
        narrator_file: ["id", "narrator_name"],
        narrator_chain_file: ["id", "sanad_id_fk", "narrator_id_fk", "position"],
        # New tables with their headers
        narrator_details_file: ["id", "narrator_id", "sect", "reliability", "titles", "patronymic"],
        narrator_death_records_file: ["id", "narrator_id", "source", "death_year"],
        narrator_evaluation_file: ["id", "narrator_id", "source", "evaluation", "summary"]
    }
    
    for file_path, headers in files_and_headers.items():
        file_exists = os.path.exists(file_path)
        file_empty = not file_exists or os.path.getsize(file_path) == 0
        
        if file_empty:
            # Create new file with headers
            try:
                with open(file_path, mode="w", newline="", encoding="utf-8") as csvfile:
                    writer = csv.writer(csvfile)
                    writer.writerow(headers)
                print(f"Created new file with headers: {file_path}")
            except Exception as e:
                print(f"Error creating file {file_path}: {str(e)}")
        else:
            print(f"File exists and is not empty: {file_path}")

# Helper function to extract narrator titles from rejal data
def extract_narrator_titles(rejal_data, ravi_id):
    titles = []
    
    if not rejal_data or not isinstance(rejal_data, dict):
        return ""
    
    data = rejal_data.get("data", {})
    if not isinstance(data, dict):
        return ""
    
    ravi_list = data.get("raviList", [])
    
    for ravi in ravi_list:
        if ravi.get("raviId") == ravi_id:
            for info in ravi.get("infoList", []):
                if info.get("title") == "لقب":  # Filter only title "لقب"
                    for text_entry in info.get("text", []):
                        book_names = []
                        for book in text_entry.get("bookName", []):
                            book_name = book.get("bookName", "")
                            if book_name:
                                book_names.append(book_name)
                        
                        text = text_entry.get("text", "")
                        if text:
                            book_names_str = ", ".join(book_names)
                            formatted_title = f"{text}({book_names_str})"
                            titles.append(formatted_title)
    
    return " | ".join(titles) if titles else ""

# Helper function to extract narrator patronymic
def extract_narrator_patronymic(rejal_data, ravi_id):
    patronymics = []
    
    if not rejal_data or not isinstance(rejal_data, dict):
        return ""
    
    data = rejal_data.get("data", {})
    if not isinstance(data, dict):
        return ""
    
    ravi_list = data.get("raviList", [])
    
    for ravi in ravi_list:
        if ravi.get("raviId") == ravi_id:
            for info in ravi.get("infoList", []):
                if info.get("title") == "کنيه":  # Filter only title "کنيه" (patronymic)
                    for text_entry in info.get("text", []):
                        book_names = []
                        for book in text_entry.get("bookName", []):
                            book_name = book.get("bookName", "")
                            if book_name:
                                book_names.append(book_name)
                        
                        text = text_entry.get("text", "")
                        if text:
                            book_names_str = ", ".join(book_names)
                            formatted_patronymic = f"{text}({book_names_str})"
                            patronymics.append(formatted_patronymic)
    
    return " | ".join(patronymics) if patronymics else ""

# Helper function to extract narrator sect and reliability
def extract_narrator_sect_reliability(rejal_data, ravi_id):
    if not rejal_data or not isinstance(rejal_data, dict):
        return {"sect": "", "reliability": ""}
    
    data = rejal_data.get("data", {})
    if not isinstance(data, dict):
        return {"sect": "", "reliability": ""}
    
    ravi_list = data.get("raviList", [])
    
    for ravi in ravi_list:
        if ravi.get("raviId") == ravi_id:
            for info in ravi.get("infoList2", []):
                if info.get("title", "").strip() == "نتيجه ارزيابي":
                    text_entries = info.get("text", [])
                    if text_entries:
                        cleaned_text = "".join(text_entries).replace(" ,", ",")
                        words = cleaned_text.split(", ")
                        
                        sect = words[0] if len(words) > 0 else ""
                        reliability = words[1] if len(words) > 1 else ""
                        
                        return {"sect": sect, "reliability": reliability}
    
    return {"sect": "", "reliability": ""}

# Helper function to extract narrator evaluation summary
def extract_narrator_evaluation_summary(rejal_data, ravi_id):
    if not rejal_data or not isinstance(rejal_data, dict):
        return ""
    
    data = rejal_data.get("data", {})
    if not isinstance(data, dict):
        return ""
    
    ravi_list = data.get("raviList", [])
    
    for ravi in ravi_list:
        if ravi.get("raviId") == ravi_id:
            for info in ravi.get("infoList2", []):
                if info.get("title") == "جمع بندي ارزيابي":  # Summary evaluation
                    text = info.get("text", "")
                    if text:
                        return text
    
    return ""

# Helper function to extract detailed evaluations
def extract_narrator_evaluations(rejal_data, ravi_id):
    evaluations = []
    
    if not rejal_data or not isinstance(rejal_data, dict):
        return evaluations
    
    data = rejal_data.get("data", {})
    if not isinstance(data, dict):
        return evaluations
    
    ravi_list = data.get("raviList", [])
    
    for ravi in ravi_list:
        if ravi.get("raviId") == ravi_id:
            for info in ravi.get("infoList2", []):
                if info.get("title") == "الفاظ جرح و تعدیل":  # Evaluation terms
                    for entry in info.get("text", []):
                        text_value = entry.get("text", "")
                        sources = []
                        
                        # Extract book names
                        if "bookName" in entry:
                            for book_item in entry["bookName"]:
                                if isinstance(book_item, dict) and "bookName" in book_item:
                                    sources.append(book_item["bookName"])
                        
                        if text_value:  # Only add if there's evaluation text
                            evaluations.append({
                                "text": text_value,
                                "sources": sources
                            })
    
    return evaluations

# Helper function to extract death information
def extract_narrator_death_info(rejal_data, ravi_id):
    death_records = []
    
    if not rejal_data or not isinstance(rejal_data, dict):
        return death_records
    
    data = rejal_data.get("data", {})
    if not isinstance(data, dict):
        return death_records
    
    ravi_list = data.get("raviList", [])
    
    for ravi in ravi_list:
        if ravi.get("raviId") == ravi_id:
            for info in ravi.get("infoList", []):
                if info.get("title") in ["وفات", "تاريخ وفات"]:  # Death or Death date
                    for entry in info.get("text", []):
                        if isinstance(entry, dict):
                            death_year = entry.get("text", "").strip()
                            book_names = []
                            
                            # Extract book names
                            if "bookName" in entry:
                                for book_item in entry.get("bookName", []):
                                    if isinstance(book_item, dict) and "bookName" in book_item:
                                        book_names.append(book_item["bookName"])
                            
                            source = ", ".join(book_names)
                            
                            if death_year or source:  # Only add if we have data
                                death_records.append({
                                    "death_year": death_year,
                                    "source": source
                                })
    
    return death_records

# Main function to process hadith data
def process_hadith_data(hadith_ids, hadith_writer, book_writer, reference_writer, 
                        sanad_writer, narrator_writer, narrator_chain_writer,
                        narrator_details_writer, narrator_death_records_writer, 
                        narrator_evaluation_writer):
    
    # Keep track of processed books and narrators to avoid duplicates
    processed_books = {}
    processed_narrators = {}
    processed_hadiths = {}  # Track processed hadiths to avoid duplicates
    processed_narrator_details = set()  # Track processed narrator details
    successful_entries = 0

    # Fetch and process data for each Hadith ID
    for hadith_id in hadith_ids:
        try:
            print(f"\n{'='*50}")
            print(f"Processing Hadith ID: {hadith_id}")
            
            # Skip if we've already processed this hadith
            if hadith_id in processed_hadiths:
                print(f"Skipping already processed Hadith ID: {hadith_id}")
                continue
                
            hadith_data = fetch_hadith_details(hadith_id)
            rejal_data = fetch_hadith_rejal(hadith_id)
            
            # Extract hadith details
            has_valid_data = False
            
            if "error" not in hadith_data and "data" in hadith_data and hadith_data["data"]:
                hadith_entry = hadith_data["data"][0]
                
                # Generate a unique UUID for this hadith
                hadith_uuid = str(uuid.uuid4())
                
                # Extract hadith ID from data
                hadith_id_from_data = hadith_entry.get("id", "N/A")
                print(f"Found hadith with ID: {hadith_id_from_data}")
                
                # Mark this hadith as processed
                processed_hadiths[hadith_id] = hadith_uuid
                
                # Extract content and clean HTML tags
                hadith_content = re.sub(r"</?[^>]+>", "", hadith_entry.get("text", "N/A"))
                
                # Extract narrators and properly join them with comma
                qaelTitleList = hadith_entry.get("qaelTitleList", ["N/A"])
                originated_from = ", ".join(qaelTitleList) if isinstance(qaelTitleList, list) else qaelTitleList

                # Extract book details and create a unique book ID based on source
                book_title = hadith_entry.get("bookTitle", "Unknown Book")
                book_source_id = hadith_entry.get("sourceId", "unknown")
                
                # Create a unique book ID or use an existing one
                if book_source_id in processed_books:
                    book_id = processed_books[book_source_id]
                    print(f"Using existing book ID: {book_id} for book: {book_title}")
                else:
                    # Generate a book ID based on source ID or create a UUID if none
                    book_id = f"book_{book_source_id}" if book_source_id != "unknown" else f"book_{str(uuid.uuid4())[:8]}"
                    
                    # Extract book metadata
                    page_num = hadith_entry.get("pageNum", "N/A")
                    volume = hadith_entry.get("vol", "N/A")
                    
                    # Write book entry only once
                    book_writer.writerow([book_id, book_title, page_num, volume])
                    processed_books[book_source_id] = book_id
                    print(f"Added new book: {book_title} with ID: {book_id}")

                # Write hadith entry with proper book ID relationship
                hadith_writer.writerow([hadith_uuid, hadith_id_from_data, hadith_content, originated_from, book_id])
                print(f"Wrote hadith entry with UUID: {hadith_uuid}")
                
                has_valid_data = True
                
                # Process references - Fixed to fetch actual reference data
                group_together_list = hadith_entry.get("groupTogetherList", [])
                print(f"Found {len(group_together_list)} references")
                
                for item in group_together_list:
                    reference_hadith_id = item.get("hadithId", "N/A")
                    
                    # Skip self-references
                    if reference_hadith_id == hadith_id_from_data:
                        print(f"Skipping self-reference to {reference_hadith_id}")
                        continue
                        
                    # Fetch the reference hadith details to get accurate metadata
                    reference_details = fetch_reference_details(reference_hadith_id)
                    
                    # If we have valid reference data
                    if reference_details:
                        reference_id = f"ref_{str(uuid.uuid4())[:8]}"  # Create unique reference ID
                        
                        reference_writer.writerow([
                            reference_id,                            # Unique reference ID
                            hadith_uuid,                             # Foreign key to hadith
                            reference_details.get("hadith_id", reference_hadith_id),  # Referenced hadith ID
                            reference_details.get("vol", "N/A"),     # Volume
                            reference_details.get("pageNum", "N/A"), # Page number
                            reference_details.get("sourceId", "N/A"), # Source ID
                            reference_details.get("sourceMainTitle", "Unknown Source")  # Source title
                        ])
                        print(f"Added reference to hadith ID: {reference_hadith_id}")
                
                # Process Sanad (Narrator Chains)
                sanad_list = []
                if rejal_data is not None and isinstance(rejal_data, dict):
                    data = rejal_data.get("data", {})
                    if isinstance(data, dict):
                        sanad_list = data.get("sanadList", [])
                
                print(f"Found {len(sanad_list)} sanad entries")
                
                # Collect all ravi IDs from this hadith for later processing
                ravi_ids = []
                
                # Process each sanad (chain of narrators)
                for sanad_list_num, sanad_entry in enumerate(sanad_list, start=1):
                    # Generate unique sanad ID using a consistent format
                    sanad_id = f"sanad_{hadith_id_from_data}_{sanad_list_num}"
                    
                    # Extract only narrators with type=0 for the sanad description
                    narrators = [sanad.get("title", "N/A") 
                                for sanad in sanad_entry.get("sanad", []) 
                                if sanad.get("type") in [0, 4]]
                    
                    # Join narrator names with spaces to create the sanad description
                    sanad_description = " ".join(narrators)
                    
                    # Write sanad entry with proper foreign key to hadith
                    sanad_writer.writerow([
                        sanad_id,           # Primary key
                        hadith_uuid,        # Foreign key to hadith
                        sanad_description,  # Full description
                        sanad_list_num      # Number/position of this sanad
                    ])
                    print(f"Added sanad #{sanad_list_num} with {len(narrators)} narrators")
                    
                    # Process each narrator in this sanad
                    position = 1  # Track position within this sanad
                    for sanad in sanad_entry.get("sanad", []):
                        # Only process narrators (type=0 or type=4)
                        if sanad.get("type") in [0, 4]:
                            narrator_name = sanad.get("title", "N/A")
                            
                            # Ensure we have a valid name
                            if narrator_name == "N/A":
                                continue
                            
                            # Extract ravi ID for additional data 
                            ravi_id = None
                            if "raviList" in sanad and len(sanad["raviList"]) > 0:
                                ravi_id = sanad["raviList"][0].get("raviId")
                                if ravi_id:
                                    ravi_ids.append(ravi_id)
                                
                            # Check if this narrator already exists in our database
                            if narrator_name in processed_narrators:
                                narrator_id = processed_narrators[narrator_name]
                                print(f"Using existing narrator: {narrator_name}")
                            else:
                                # Create a numeric ID for narrators instead of a string-based one
                                narrator_id = str(uuid.uuid4().int)[:8]  # Using first 8 digits of uuid integer
                                
                                # Add narrator to the database
                                narrator_writer.writerow([narrator_id, narrator_name])
                                processed_narrators[narrator_name] = narrator_id
                                print(f"Added new narrator: {narrator_name}")
                            
                            # Create a chain entry linking this narrator to this sanad
                            chain_id = f"chain_{sanad_id}_{position}"
                            narrator_chain_writer.writerow([
                                chain_id,     # Primary key
                                sanad_id,     # Foreign key to sanad
                                narrator_id,  # Foreign key to narrator - FIXED: Now using the numeric ID
                                position      # Position in the chain
                            ])
                            
                            # Process additional narrator details if we have a ravi ID and haven't processed this narrator yet
                            if ravi_id and (narrator_id, ravi_id) not in processed_narrator_details:
                                # Extract narrator titles
                                titles = extract_narrator_titles(rejal_data, ravi_id)
                                
                                # Extract narrator patronymic
                                patronymic = extract_narrator_patronymic(rejal_data, ravi_id)
                                
                                # Extract sect and reliability
                                sect_reliability = extract_narrator_sect_reliability(rejal_data, ravi_id)
                                
                                # Generate IDs for new records
                                details_id = f"details_{str(uuid.uuid4())[:8]}"
                                
                                # Write narrator details
                                narrator_details_writer.writerow([
                                    details_id,
                                    narrator_id,
                                    sect_reliability.get("sect", ""),
                                    sect_reliability.get("reliability", ""),
                                    titles,
                                    patronymic  # Now including the patronymic
                                ])
                                
                                # Mark this narrator as processed for additional details
                                processed_narrator_details.add((narrator_id, ravi_id))
                                
                                print(f"Added details for narrator: {narrator_name}")
                                
                                # Extract death records using the improved function
                                death_records = extract_narrator_death_info(rejal_data, ravi_id)
                                for death_record in death_records:
                                    death_record_id = f"death_{str(uuid.uuid4())[:8]}"
                                    narrator_death_records_writer.writerow([
                                        death_record_id,
                                        narrator_id,
                                        death_record.get("source", ""),
                                        death_record.get("death_year", "")
                                    ])
                                    print(f"Added death record for narrator: {narrator_name}")
                                
                                # Extract evaluation summary
                                summary = extract_narrator_evaluation_summary(rejal_data, ravi_id)
                                
                                # Extract detailed evaluations
                                evaluations = extract_narrator_evaluations(rejal_data, ravi_id)
                                for evaluation in evaluations:
                                    eval_id = f"eval_{str(uuid.uuid4())[:8]}"
                                    source = ", ".join(evaluation.get("sources", []))
                                    
                                    narrator_evaluation_writer.writerow([
                                        eval_id,
                                        narrator_id,
                                        source,
                                        evaluation.get("text", ""),
                                        summary  # Including the evaluation summary
                                    ])
                                    print(f"Added evaluation for narrator: {narrator_name}")
                                
                                # If we have a summary but no detailed evaluations, still add a record
                                if summary and not evaluations:
                                    eval_id = f"eval_{str(uuid.uuid4())[:8]}"
                                    narrator_evaluation_writer.writerow([
                                        eval_id,
                                        narrator_id,
                                        "",  # No source
                                        "",  # No evaluation text
                                        summary  # Only summary
                                    ])
                                    print(f"Added evaluation summary for narrator: {narrator_name}")
                            
                            # Increment position for the next narrator in this chain
                            position += 1
                    
                    print(f"Added {position-1} narrators to the chain for sanad #{sanad_list_num}")
                
                successful_entries += 1
            else:
                print(f"No valid data found for Hadith ID: {hadith_id}")
                logging.warning(f"No valid data found for Hadith ID: {hadith_id}")
            
            if has_valid_data:
                print(f"Successfully processed Hadith ID: {hadith_id}")
            
            # Delay to avoid bans
            delay = random.uniform(2, 5)
            print(f"Waiting {delay:.2f} seconds before next request...")
            time.sleep(delay)
            
        except Exception as e:
            logging.error(f"Error processing Hadith ID {hadith_id}: {str(e)}")
            print(f"⚠️ Error processing Hadith ID {hadith_id}: {str(e)}")
            # Continue with the next ID instead of stopping
            continue
            
    return successful_entries

# Main execution
def main():
    print("Starting script execution...")
    hadith_ids = extract_hadith_ids(sitemap_file)

    # Check if we have any valid IDs
    if not hadith_ids:
        logging.error("No valid hadith IDs found in the sitemap file.")
        print("❌ No valid hadith IDs found. Please check the sitemap file.")
        return 1

    # Initialize CSV files with headers if needed
    initialize_csv_files()

    # Process limit for testing (remove in production)
    process_limit = 5  # Process only first 50 IDs for testing
    hadith_ids = hadith_ids[:process_limit]
    print(f"Will process {len(hadith_ids)} hadith IDs for testing purposes")

    try:
        # Open CSV files in append mode
        with open(hadith_file, mode="a", newline="", encoding="utf-8") as hadith_csv, \
             open(book_file, mode="a", newline="", encoding="utf-8") as book_csv, \
             open(reference_file, mode="a", newline="", encoding="utf-8") as reference_csv, \
             open(sanad_file, mode="a", newline="", encoding="utf-8") as sanad_csv, \
             open(narrator_file, mode="a", newline="", encoding="utf-8") as narrator_csv, \
             open(narrator_chain_file, mode="a", newline="", encoding="utf-8") as narrator_chain_csv, \
             open(narrator_details_file, mode="a", newline="", encoding="utf-8") as narrator_details_csv, \
             open(narrator_death_records_file, mode="a", newline="", encoding="utf-8") as narrator_death_records_csv, \
             open(narrator_evaluation_file, mode="a", newline="", encoding="utf-8") as narrator_evaluation_csv:
            
            hadith_writer = csv.writer(hadith_csv)
            book_writer = csv.writer(book_csv)
            reference_writer = csv.writer(reference_csv)
            sanad_writer = csv.writer(sanad_csv)
            narrator_writer = csv.writer(narrator_csv)
            narrator_chain_writer = csv.writer(narrator_chain_csv)
            narrator_details_writer = csv.writer(narrator_details_csv)
            narrator_death_records_writer = csv.writer(narrator_death_records_csv)
            narrator_evaluation_writer = csv.writer(narrator_evaluation_csv)
            
            # Process the data
            successful_entries = process_hadith_data(
                hadith_ids, 
                hadith_writer, book_writer, reference_writer, 
                sanad_writer, narrator_writer, narrator_chain_writer,
                narrator_details_writer, narrator_death_records_writer, narrator_evaluation_writer
            )

            print(f"\n✅ Processed {successful_entries} out of {len(hadith_ids)} hadith entries successfully.")
            
            # Verify files were written
            for file_path in [hadith_file, book_file, reference_file, sanad_file, narrator_file, narrator_chain_file,
                             narrator_details_file, narrator_death_records_file, narrator_evaluation_file]:
                if os.path.exists(file_path):
                    size = os.path.getsize(file_path)
                    print(f"File {os.path.basename(file_path)}: {size} bytes")
                    
                    # Read a few lines to verify content
                    if size > 0:
                        with open(file_path, 'r', encoding='utf-8') as f:
                            lines = f.readlines()
                            line_count = len(lines)
                            print(f"  - Contains {line_count} lines")
                            if line_count > 1:
                                print(f"  - First data row: {lines[1].strip()}")
                else:
                    print(f"File {os.path.basename(file_path)} does not exist")
        
    except Exception as e:
        logging.error(f"Critical error: {str(e)}")
        print(f"❌ Critical error: {str(e)}")
        return 1

    print(f"\nAll output has been saved to: {log_file_path}")
    return 0

# Execute main function
if __name__ == "__main__":
    sys.exit(main())