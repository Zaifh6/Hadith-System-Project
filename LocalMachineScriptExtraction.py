import csv
import uuid
import re
import os
import json
import logging
import sys
from datetime import datetime
import traceback

# Create a log file name with timestamp
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_file_path = f"hadith_processing_log_{timestamp}.txt"
skipped_files_log = f"skipped_files_log_{timestamp}.txt"  # New log for skipped files

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

# Configure logging
logging.basicConfig(
    filename="hadith_processing.log", 
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# JSON folder containing all JSON files to process
json_folder = r"X:\hadith_scraping_script\hadith_scraping_script\scraped_hadith_json"
csv_folder = r"X:\hadith_scraping_script\hadith_scraping_script\csv-data-of-json"

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
hadith_content_file = os.path.join(csv_folder, "hadith_content.csv")  # New file for hadith content

# Print file paths for debugging
print(f"CSV files will be saved to: {csv_folder}")
for file_path in [hadith_file, book_file, reference_file, sanad_file, narrator_file, narrator_chain_file, 
                 narrator_details_file, narrator_death_records_file, narrator_evaluation_file, hadith_content_file]:
    print(f"File path: {file_path}")
    print(f"  - Directory exists: {os.path.exists(os.path.dirname(file_path))}")
    print(f"  - File exists: {os.path.exists(file_path)}")

# Extract Hadith IDs from JSON files in the folder
def extract_hadith_ids_from_files():
    try:
        hadith_ids = set()
        
        # First, check if the folder exists
        if not os.path.exists(json_folder):
            print(f"Error: JSON folder does not exist: {json_folder}")
            return []
            
        # Print folder information
        print(f"Scanning JSON folder: {json_folder}")
        
        # Count total JSON files
        json_files = [f for f in os.listdir(json_folder) if f.endswith('.json')]
        total_files = len(json_files)
        print(f"Found {total_files} JSON files in the folder")
        
        # Process each file
        for i, filename in enumerate(json_files, 1):
            if i % 10 == 0 or i == 1 or i == total_files:
                print(f"Processing file {i}/{total_files}: {filename}")
                
            if filename.startswith("hadith_") and filename.endswith(".json"):
                # Extract the ID from the filename
                match = re.search(r'hadith_(\d+)\.json', filename)
                if match:
                    hadith_ids.add(match.group(1))
        
        hadith_ids = list(hadith_ids)
        print(f"Found {len(hadith_ids)} unique hadith IDs from JSON files")
        if hadith_ids and len(hadith_ids) > 0:
            sample_size = min(5, len(hadith_ids))
            print(f"Sample of {sample_size} IDs: {hadith_ids[:sample_size]}")
        return hadith_ids
    except Exception as e:
        logging.error(f"Error extracting hadith IDs from JSON files: {str(e)}")
        print(f"Exception reading JSON files: {str(e)}")
        traceback.print_exc()
        return []

# Function to load hadith details from JSON file
def load_hadith_details(hadith_id):
    file_path = os.path.join(json_folder, f"hadith_{hadith_id}.json")
    
    try:
        if os.path.exists(file_path):
            print(f"Loading hadith details from file: {file_path}")
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            print(f"Successfully loaded hadith details for ID: {hadith_id}")
            
            # The data structure is different - we need to extract from hadith_details
            if "hadith_details" in data:
                return data["hadith_details"]
            return data
        else:
            print(f"File not found: {file_path}")
            logging.error(f"Hadith details file not found for ID {hadith_id}: {file_path}")
            return {"error": "File not found"}
    except Exception as e:
        logging.error(f"Exception while loading hadith details for ID {hadith_id}: {str(e)}")
        return {"error": str(e)}

# Function to load reference details from JSON file
def load_reference_details(reference_hadith_id):
    file_path = os.path.join(json_folder, f"hadith_{reference_hadith_id}.json")
    
    try:
        if os.path.exists(file_path):
            print(f"Loading reference details from file: {file_path}")
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Extract from hadith_details
            if "hadith_details" in data and "data" in data["hadith_details"]:
                ref_data = data["hadith_details"]["data"][0]
                return {
                    "hadith_id": ref_data.get("id", "N/A"),
                    "vol": ref_data.get("vol", "N/A"),
                    "pageNum": ref_data.get("pageNum", "N/A"),
                    "sourceId": ref_data.get("sourceId", "N/A"),
                    "sourceMainTitle": ref_data.get("bookTitle", "Unknown Source")
                }
            return {}
        else:
            print(f"Reference details file not found: {file_path}")
            logging.error(f"Reference details file not found for ID {reference_hadith_id}: {file_path}")
            return {}
    except Exception as e:
        logging.error(f"Exception while loading reference details for ID {reference_hadith_id}: {str(e)}")
        return {}

# Function to load hadith rejal data from JSON file
def load_hadith_rejal(hadith_id):
    file_path = os.path.join(json_folder, f"hadith_{hadith_id}.json")
    
    try:
        if os.path.exists(file_path):
            print(f"Loading hadith rejal from file: {file_path}")
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            print(f"Successfully loaded hadith rejal for ID: {hadith_id}")
            
            # The data structure is different - we need to extract from hadith_rejal_list
            if "hadith_rejal_list" in data:
                return data["hadith_rejal_list"]
            return None
        else:
            print(f"File not found: {file_path}")
            logging.error(f"Hadith rejal file not found for ID {hadith_id}: {file_path}")
            return None
    except Exception as e:
        logging.error(f"Exception while loading hadith rejal for ID {hadith_id}: {str(e)}")
        return None

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
                        
                        # Extract book names and ravi titles
                        if "bookName" in entry:
                            for book_item in entry["bookName"]:
                                if isinstance(book_item, dict):
                                    book_title = book_item.get("bookName", "")
                                    ravi_title = book_item.get("raviTitle", "")
                                    
                                    # Format: raviTitle (bookTitle)
                                    formatted_source = f"{ravi_title} ({book_title})" if ravi_title and book_title else ravi_title or book_title
                                    sources.append(formatted_source)
                        
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

# Check if CSV files exist and create headers if needed
def initialize_csv_files():
    files_and_headers = {
        hadith_file: ["uuid", "hadith_id", "hadith_content_id", "originated_from", "book_id", "page_num", "volume"],
        book_file: ["id", "title"],
        reference_file: ["id", "hadith_uuid_fk", "hadith_id", "volume", "page_num", "source_id", "source_title"],
        sanad_file: ["id", "hadith_uuid_fk", "sanad_description", "sanad_number"],
        narrator_file: ["id", "narrator_name"],
        narrator_chain_file: ["id", "sanad_id_fk", "narrator_id_fk", "position"],
        # New tables with their headers
        narrator_details_file: ["id", "narrator_id", "sect", "reliability", "titles", "patronymic"],
        narrator_death_records_file: ["id", "narrator_id", "source", "death_year"],
        narrator_evaluation_file: ["id", "narrator_id", "source", "evaluation", "summary"],
        hadith_content_file: ["id", "content"]  # New table for hadith content
    }
    
    # First, read existing content IDs from hadith_content table if it exists
    existing_content_ids = {}
    if os.path.exists(hadith_content_file) and os.path.getsize(hadith_content_file) > 0:
        try:
            with open(hadith_content_file, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                next(reader)  # Skip header
                for row in reader:
                    if len(row) >= 2:
                        content_id, content = row[0], row[1]
                        existing_content_ids[content] = content_id
            print(f"Loaded {len(existing_content_ids)} existing content IDs from hadith_content file")
        except Exception as e:
            print(f"Error reading existing content IDs: {str(e)}")
    
    # Now read existing hadith records to ensure we don't lose existing references
    existing_hadith_content_refs = {}
    if os.path.exists(hadith_file) and os.path.getsize(hadith_file) > 0:
        try:
            with open(hadith_file, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                next(reader)  # Skip header
                for row in reader:
                    if len(row) >= 3:
                        hadith_id, content_id = row[1], row[2]
                        existing_hadith_content_refs[hadith_id] = content_id
            print(f"Loaded {len(existing_hadith_content_refs)} existing hadith-to-content references")
        except Exception as e:
            print(f"Error reading existing hadith references: {str(e)}")
    
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
    
    return existing_content_ids, existing_hadith_content_refs

# Main execution
def main():
    print("Starting script execution...")
    
    # Initialize CSV files with headers if needed and get existing content mappings
    existing_content_ids, existing_hadith_content_refs = initialize_csv_files()
    
    # Create a file to track skipped or failed files
    skipped_files_path = os.path.join(csv_folder, skipped_files_log)
    with open(skipped_files_path, 'w', encoding='utf-8') as skipped_file:
        skipped_file.write("Filename,Reason,Timestamp\n")  # Write CSV header
    
    # Check if the JSON file exists
    if not os.path.exists(json_folder):
        logging.error(f"JSON folder does not exist: {json_folder}")
        print(f"❌ JSON folder does not exist: {json_folder}")
        return 1
        
    print(f"Processing JSON folder: {json_folder}")
    
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
             open(narrator_evaluation_file, mode="a", newline="", encoding="utf-8") as narrator_evaluation_csv, \
             open(hadith_content_file, mode="a", newline="", encoding="utf-8") as hadith_content_csv:
            
            hadith_writer = csv.writer(hadith_csv)
            book_writer = csv.writer(book_csv)
            reference_writer = csv.writer(reference_csv)
            sanad_writer = csv.writer(sanad_csv)
            narrator_writer = csv.writer(narrator_csv)
            narrator_chain_writer = csv.writer(narrator_chain_csv)
            narrator_details_writer = csv.writer(narrator_details_csv)
            narrator_death_records_writer = csv.writer(narrator_death_records_csv)
            narrator_evaluation_writer = csv.writer(narrator_evaluation_csv)
            hadith_content_writer = csv.writer(hadith_content_csv)
            
            # Keep track of processed items
            processed_books = {}
            processed_narrators = {}
            processed_narrator_details = set()  # Track narrator details with (narrator_id, sect, reliability, titles, patronymic) keys
            processed_narrator_death_records = set()  # Track death records with (narrator_id, year, source) keys
            processed_narrator_evaluations = set()  # Track evaluations with (narrator_id, text, source) keys
            processed_hadith_contents = existing_content_ids.copy()
            
            try:
                print("\n" + "="*50)
                print(f"Processing folder: {json_folder}")
                
                # Load data from folder
                try:
                    hadith_ids = extract_hadith_ids_from_files()
                except Exception as e:
                    print(f"Error extracting hadith IDs: {str(e)}")
                    with open(skipped_files_path, 'a', encoding='utf-8') as skipped_file:
                        skipped_file.write(f"{os.path.basename(json_folder)},Error extracting hadith IDs: {str(e)},{datetime.now().isoformat()}\n")
                    return 1
                
                # Process each hadith
                for hadith_id in hadith_ids:
                    print(f"\nProcessing Hadith ID: {hadith_id}")
                    
                    # Load data from file
                    try:
                        hadith_details = load_hadith_details(hadith_id)
                        rejal_data = load_hadith_rejal(hadith_id)
                    except Exception as e:
                        print(f"Error loading data for Hadith ID {hadith_id}: {str(e)}")
                        with open(skipped_files_path, 'a', encoding='utf-8') as skipped_file:
                            skipped_file.write(f"hadith_{hadith_id}.json,Error loading data: {str(e)},{datetime.now().isoformat()}\n")
                        continue
                    
                    # Process hadith data
                    has_valid_data = False
                    
                    if "data" in hadith_details and hadith_details["data"]:
                        hadith_entry = hadith_details["data"][0]
                        
                        # Generate a unique UUID for this hadith
                        hadith_uuid = str(uuid.uuid4())
                        
                        # Extract hadith ID from data
                        hadith_id_from_data = hadith_entry.get("id", "N/A")
                        print(f"Found hadith with ID: {hadith_id_from_data}")
                        
                        # Extract content and clean HTML tags
                        hadith_content = re.sub(r"</?[^>]+>", "", hadith_entry.get("text", "N/A"))
                        
                        # Check if this hadith ID already has a content reference (from previous runs)
                        if hadith_id_from_data in existing_hadith_content_refs:
                            hadith_content_id = existing_hadith_content_refs[hadith_id_from_data]
                            print(f"Using existing content reference for hadith {hadith_id_from_data}: {hadith_content_id}")
                        # Check if this hadith content already exists
                        elif hadith_content in processed_hadith_contents:
                            hadith_content_id = processed_hadith_contents[hadith_content]
                            print(f"Using existing hadith content ID: {hadith_content_id}")
                        else:
                            # Generate a content ID with a UUID that is shorter but still unique
                            short_uuid = str(uuid.uuid4()).split('-')[0]
                            hadith_content_id = f"content_{short_uuid}"
                            
                            # Write content entry
                            hadith_content_writer.writerow([hadith_content_id, hadith_content])
                            processed_hadith_contents[hadith_content] = hadith_content_id
                            print(f"Added new hadith content with ID: {hadith_content_id}")
                        
                        # Extract narrators and properly join them with comma
                        qaelTitleList = hadith_entry.get("qaelTitleList", ["N/A"])
                        originated_from = ", ".join(qaelTitleList) if isinstance(qaelTitleList, list) else qaelTitleList
                        
                        # Extract book details and create a unique book ID based on source
                        book_title = hadith_entry.get("bookTitle", "Unknown Book")
                        book_source_id = hadith_entry.get("sourceId", "unknown")
                        
                        # Extract page_num and volume for hadith table
                        page_num = hadith_entry.get("pageNum", "N/A")
                        volume = hadith_entry.get("vol", "N/A")
                        
                        # Create a unique book ID or use an existing one
                        if book_title in processed_books:
                            book_id = processed_books[book_title]
                            print(f"Using existing book ID: {book_id} for book: {book_title}")
                        else:
                            # Generate a book ID based on source ID or create a UUID if none
                            book_id = f"book_{book_source_id}" if book_source_id != "unknown" else f"book_{str(uuid.uuid4())[:8]}"
                            
                            # Write book entry only once - with just the title
                            book_writer.writerow([book_id, book_title])
                            processed_books[book_title] = book_id
                            print(f"Added new book: {book_title} with ID: {book_id}")
                        
                        # Write hadith entry with proper book ID relationship, content ID reference, and page/volume
                        hadith_writer.writerow([hadith_uuid, hadith_id_from_data, hadith_content_id, originated_from, book_id, page_num, volume])
                        print(f"Wrote hadith entry with UUID: {hadith_uuid}")
                        
                        has_valid_data = True
                        
                        # Process references
                        group_together_list = hadith_entry.get("groupTogetherList", [])
                        print(f"Found {len(group_together_list)} references")
                        
                        for item in group_together_list:
                            reference_hadith_id = item.get("hadithId", "N/A")
                            
                            # Skip self-references
                            if reference_hadith_id == hadith_id_from_data:
                                print(f"Skipping self-reference to {reference_hadith_id}")
                                continue
                            
                            # Create reference entry directly from the data we have
                            reference_id = f"ref_{str(uuid.uuid4())[:8]}"
                            
                            reference_writer.writerow([
                                reference_id,                       # Unique reference ID
                                hadith_uuid,                        # Foreign key to hadith
                                reference_hadith_id,                # Referenced hadith ID
                                item.get("vol", "N/A"),            # Volume
                                item.get("pageNum", "N/A"),        # Page number
                                item.get("sourceId", "N/A"),       # Source ID
                                item.get("sourceMainTitle", "Unknown Source")  # Source title
                            ])
                            print(f"Added reference to hadith ID: {reference_hadith_id}")
                        
                        # Process Sanad (Narrator Chains) using the new logic
                        sanad_lists = []
                        if rejal_data and isinstance(rejal_data, dict):
                            data = rejal_data.get("data", {})
                            if isinstance(data, dict):
                                sanad_lists = data.get("sanadList", [])
                        
                        print(f"Found {len(sanad_lists)} sanad entries")
                        
                        # Process each sanad (chain of narrators)
                        for sanad_list_num, sanad_entry in enumerate(sanad_lists, start=1):
                            # Generate unique sanad ID using a consistent format
                            sanad_id = f"sanad_{hadith_id_from_data}_{sanad_list_num}"
                            
                            # Extract the sanad description based on the new logic
                            sanad = sanad_entry.get("sanad", [])
                            sanad_description = " ".join(item.get("title", "") for item in sanad if item.get("title"))
                            
                            # Write sanad entry with proper foreign key to hadith
                            sanad_writer.writerow([
                                sanad_id,           # Primary key
                                hadith_uuid,        # Foreign key to hadith
                                sanad_description,  # Full description
                                sanad_list_num      # Number/position of this sanad
                            ])
                            print(f"Added sanad #{sanad_list_num} with description: {sanad_description[:50]}...")
                            
                            # Process each narrator in this sanad
                            position = 1  # Track position within this sanad
                            for sanad_item in sanad_entry.get("sanad", []):
                                # Only process narrators (type=0 or type=4)
                                if sanad_item.get("type") in [0, 4]:
                                    narrator_name = sanad_item.get("title", "N/A")
                                    
                                    # Ensure we have a valid name
                                    if narrator_name == "N/A":
                                        continue
                                    
                                    # Extract all ravi IDs for this title
                                    ravi_ids = []
                                    ravi_names = {}  # Store actual narrator names by raviId
                                    if "raviList" in sanad_item:
                                        for ravi_entry in sanad_item["raviList"]:
                                            if "raviId" in ravi_entry:
                                                ravi_id = ravi_entry.get("raviId")
                                                ravi_ids.append(ravi_id)
                                                # Look for the actual narrator name in the rejal data
                                                for ravi in rejal_data.get("data", {}).get("raviList", []):
                                                    if ravi.get("raviId") == ravi_id:
                                                        actual_name = ravi.get("raviTitle", "")
                                                        if actual_name:
                                                            ravi_names[ravi_id] = actual_name
                                    
                                    # Log the found ravi IDs
                                    if len(ravi_ids) > 1:
                                        print(f"Found multiple raviIDs for '{narrator_name}': {ravi_ids}")
                                        print(f"Actual narrator names: {ravi_names}")
                                    elif len(ravi_ids) == 1:
                                        print(f"Found single raviID for '{narrator_name}': {ravi_ids[0]}")
                                        if ravi_ids[0] in ravi_names:
                                            print(f"Actual narrator name: {ravi_names[ravi_ids[0]]}")
                                    else:
                                        print(f"No raviID found for '{narrator_name}'")
                                    
                                    # Process each raviId separately - always get actual narrators from raviTitle
                                    for ravi_id in ravi_ids if ravi_ids else [None]:
                                        # Get the actual narrator name from raviTitle
                                        actual_narrator_name = ravi_names.get(ravi_id, "") if ravi_id else ""
                                        
                                        # If we have an actual narrator name, use it; otherwise use the title
                                        current_narrator_name = actual_narrator_name if actual_narrator_name else narrator_name
                                        
                                        # For cases with multiple ravi IDs or generic names like "عدة من أصحابنا",
                                        # append the hadith ID to make the narrator name unique per hadith
                                        if len(ravi_ids) > 1 or (not actual_narrator_name and narrator_name in ["عدة من أصحابنا", "N/A"]):
                                            current_narrator_name = f"{current_narrator_name}_{hadith_id_from_data}"
                                            print(f"Created unique narrator name with hadith ID: {current_narrator_name}")
                                    
                                        # Check if this narrator already exists in our database
                                        if current_narrator_name in processed_narrators:
                                            narrator_id = processed_narrators[current_narrator_name]
                                            print(f"Using existing narrator: {current_narrator_name}")
                                        else:
                                            # Create a numeric ID for narrators instead of a string-based one
                                            narrator_id = str(uuid.uuid4().int)[:8]  # Using first 8 digits of uuid integer
                                            
                                            # Add narrator to the database
                                            narrator_writer.writerow([narrator_id, current_narrator_name])
                                            processed_narrators[current_narrator_name] = narrator_id
                                            print(f"Added new narrator: {current_narrator_name} (from title: {narrator_name})")
                                        
                                        # Create a chain entry linking this narrator to this sanad
                                        chain_id = f"chain_{sanad_id}_{position}_{ravi_id if ravi_id else ''}"
                                        narrator_chain_writer.writerow([
                                            chain_id,     # Primary key
                                            sanad_id,     # Foreign key to sanad
                                            narrator_id,  # Foreign key to narrator
                                            position      # Position in the chain
                                        ])
                                        
                                        # Process additional narrator details if we have a ravi ID
                                        if ravi_id:
                                            # Extract narrator details
                                            titles = extract_narrator_titles(rejal_data, ravi_id)
                                            patronymic = extract_narrator_patronymic(rejal_data, ravi_id)
                                            sect_reliability = extract_narrator_sect_reliability(rejal_data, ravi_id)
                                            
                                            # Create a unique key for this narrator detail
                                            detail_key = (
                                                narrator_id,
                                                sect_reliability.get("sect", ""),
                                                sect_reliability.get("reliability", ""),
                                                titles,
                                                patronymic
                                            )
                                            
                                            # Only add if this specific detail combination doesn't exist for this narrator
                                            if detail_key not in processed_narrator_details:
                                                details_id = f"details_{str(uuid.uuid4())[:8]}"
                                                narrator_details_writer.writerow([
                                                    details_id,
                                                    narrator_id,
                                                    sect_reliability.get("sect", ""),
                                                    sect_reliability.get("reliability", ""),
                                                    titles,
                                                    patronymic
                                                ])
                                                processed_narrator_details.add(detail_key)
                                                print(f"Added details for narrator: {current_narrator_name} (Ravi ID: {ravi_id})")
                                            else:
                                                print(f"Skipping duplicate details for narrator: {current_narrator_name}")
                                            
                                            # Process death records (one-to-many relationship)
                                            death_records = extract_narrator_death_info(rejal_data, ravi_id)
                                            for death_record in death_records:
                                                death_year = death_record.get("death_year", "")
                                                source = death_record.get("source", "")
                                                
                                                # Create a unique key for this death record
                                                death_record_key = (narrator_id, death_year, source)
                                                
                                                # Only add if this specific death record doesn't exist for this narrator
                                                if death_record_key not in processed_narrator_death_records:
                                                    death_record_id = f"death_{str(uuid.uuid4())[:8]}"
                                                    narrator_death_records_writer.writerow([
                                                        death_record_id,
                                                        narrator_id,
                                                        source,
                                                        death_year
                                                    ])
                                                    processed_narrator_death_records.add(death_record_key)
                                                    print(f"Added death record for narrator: {current_narrator_name} (Ravi ID: {ravi_id})")
                                                else:
                                                    print(f"Skipping duplicate death record for narrator: {current_narrator_name}")
                                            
                                            # Extract evaluation summary
                                            summary = extract_narrator_evaluation_summary(rejal_data, ravi_id)
                                            
                                            # Extract detailed evaluations (one-to-many relationship)
                                            evaluations = extract_narrator_evaluations(rejal_data, ravi_id)
                                            for evaluation in evaluations:
                                                eval_text = evaluation.get("text", "")
                                                source = ", ".join(evaluation.get("sources", []))
                                                
                                                # Create a unique key for this evaluation
                                                eval_key = (narrator_id, eval_text, source)
                                                
                                                # Only add if this specific evaluation doesn't exist for this narrator
                                                if eval_key not in processed_narrator_evaluations:
                                                    eval_id = f"eval_{str(uuid.uuid4())[:8]}"
                                                    narrator_evaluation_writer.writerow([
                                                        eval_id,
                                                        narrator_id,
                                                        source,
                                                        eval_text,
                                                        summary
                                                    ])
                                                    processed_narrator_evaluations.add(eval_key)
                                                    print(f"Added evaluation for narrator: {current_narrator_name} (Ravi ID: {ravi_id})")
                                                else:
                                                    print(f"Skipping duplicate evaluation for narrator: {current_narrator_name}")
                                            
                                            # If we have a summary but no detailed evaluations, still add a record
                                            if summary and not evaluations:
                                                eval_key = (narrator_id, "", "")  # Empty text and source
                                                if eval_key not in processed_narrator_evaluations:
                                                    eval_id = f"eval_{str(uuid.uuid4())[:8]}"
                                                    narrator_evaluation_writer.writerow([
                                                        eval_id,
                                                        narrator_id,
                                                        "",  # No source
                                                        "",  # No evaluation text
                                                        summary  # Only summary
                                                    ])
                                                    processed_narrator_evaluations.add(eval_key)
                                                    print(f"Added evaluation summary for narrator: {current_narrator_name} (Ravi ID: {ravi_id})")
                                                else:
                                                    print(f"Skipping duplicate summary-only evaluation for narrator: {current_narrator_name}")
                                    
                                    # Increment position for the next narrator in this chain
                                    position += 1
                            
                            print(f"Added {position-1} narrators to the chain for sanad #{sanad_list_num}")
                        
                        print(f"Successfully processed Hadith ID: {hadith_id_from_data}")
                    else:
                        print(f"No valid data found for Hadith ID: {hadith_id}")
                        with open(skipped_files_path, 'a', encoding='utf-8') as skipped_file:
                            skipped_file.write(f"hadith_{hadith_id}.json,No valid data found,{datetime.now().isoformat()}\n")
            
                print(f"\nFinished processing all hadith files.")
                print(f"Total hadiths processed: {len(hadith_ids)}")
            
            except Exception as e:
                logging.error(f"Error processing folder: {str(e)}")
                print(f"⚠️ Error processing folder: {str(e)}")
                traceback.print_exc()
                with open(skipped_files_path, 'a', encoding='utf-8') as skipped_file:
                    skipped_file.write(f"folder_processing,Processing error: {str(e).replace(',', ';')},{datetime.now().isoformat()}\n")
                return 1

            # Verify files were written
            for file_path in [hadith_file, book_file, reference_file, sanad_file, narrator_file, narrator_chain_file,
                             narrator_details_file, narrator_death_records_file, narrator_evaluation_file, hadith_content_file]:
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
        traceback.print_exc()
        return 1

    print(f"\nAll output has been saved to: {log_file_path}")
    print(f"Skipped files log saved to: {skipped_files_path}")
    return 0

# Execute main function
if __name__ == "__main__":
    sys.exit(main())

