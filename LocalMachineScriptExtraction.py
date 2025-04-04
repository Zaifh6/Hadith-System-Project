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

# Folder containing JSON responses
json_folder = r"C:\Users\User\Downloads\data\JSON Scraped"
csv_folder = r"C:\Users\User\Downloads\data\CSV Data"

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

# Check if CSV files exist and create headers if needed
def initialize_csv_files():
    files_and_headers = {
        hadith_file: ["uuid", "hadith_id", "hadith_content_id", "originated_from", "book_id"],
        book_file: ["id", "title", "page_num", "volume"],
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

# Main execution
def main():
    print("Starting script execution...")
    
    # Initialize CSV files with headers if needed
    initialize_csv_files()
    
    # Create a file to track skipped or failed files
    skipped_files_path = os.path.join(csv_folder, skipped_files_log)
    with open(skipped_files_path, 'w', encoding='utf-8') as skipped_file:
        skipped_file.write("Filename,Reason,Timestamp\n")  # Write CSV header
    
    # Get all JSON files in the folder
    if not os.path.exists(json_folder):
        logging.error(f"JSON folder does not exist: {json_folder}")
        print(f"❌ JSON folder does not exist: {json_folder}")
        return 1
    
    # First, get ALL json files without filtering
    all_json_files = [f for f in os.listdir(json_folder) if f.endswith('.json')]
    print(f"Found {len(all_json_files)} total JSON files in the folder")
    
    # Then, get files that match our expected pattern
    json_files = [f for f in all_json_files if f.startswith('hadith_')]
    total_files = len(json_files)
    
    # Log files that don't match our pattern
    non_hadith_files = set(all_json_files) - set(json_files)
    if non_hadith_files:
        print(f"Found {len(non_hadith_files)} JSON files that don't start with 'hadith_'")
        with open(os.path.join(csv_folder, skipped_files_log), 'a', encoding='utf-8') as skipped_file:
            for filename in non_hadith_files:
                skipped_file.write(f"{filename},Filename doesn't match expected pattern 'hadith_*.json',{datetime.now().isoformat()}\n")
    
    if total_files == 0:
        logging.error("No hadith JSON files found in the folder.")
        print("❌ No hadith JSON files found. Please check the JSON folder.")
        return 1
        
    print(f"Found {total_files} hadith JSON files to process")
    
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
            processed_hadiths = {}
            processed_narrator_details = set()
            processed_hadith_contents = {}  # Track processed hadith contents
            successful_entries = 0
            
            # Process each file
            for i, filename in enumerate(json_files, 1):
                try:
                    print(f"\n{'='*50}")
                    print(f"Processing file {i}/{len(json_files)}: {filename}")
                    
                    # Extract hadith ID from filename
                    match = re.search(r'hadith_(\d+)\.json', filename)
                    if not match:
                        print(f"Could not extract hadith ID from filename: {filename}")
                        continue
                        
                    hadith_id = match.group(1)
                    
                    # Skip if we've already processed this hadith
                    if hadith_id in processed_hadiths:
                        print(f"Skipping already processed Hadith ID: {hadith_id}")
                        continue
                    
                    # Load data from file
                    file_path = os.path.join(json_folder, filename)
                    try:
                        with open(file_path, 'r', encoding='utf-8') as f:
                            file_data = json.load(f)
                    except Exception as e:
                        print(f"Error reading file {filename}: {str(e)}")
                        with open(os.path.join(csv_folder, skipped_files_log), 'a', encoding='utf-8') as skipped_file:
                            skipped_file.write(f"{filename},Error reading file: {str(e)},{datetime.now().isoformat()}\n")
                        continue
                    
                    # Extract hadith details and rejal data
                    hadith_data = file_data.get("hadith_details", {})
                    rejal_data = file_data.get("hadith_rejal_list", {})
                    
                    # Process hadith data
                    has_valid_data = False
                    
                    if "data" in hadith_data and hadith_data["data"]:
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
                        
                        # Check if this hadith content already exists
                        if hadith_content in processed_hadith_contents:
                            hadith_content_id = processed_hadith_contents[hadith_content]
                            print(f"Using existing hadith content ID: {hadith_content_id}")
                        else:
                            # Generate a content ID
                            hadith_content_id = f"content_{str(uuid.uuid4())[:8]}"
                            
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
                        
                        # Write hadith entry with proper book ID relationship and content ID reference
                        hadith_writer.writerow([hadith_uuid, hadith_id_from_data, hadith_content_id, originated_from, book_id])
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
                        
                        # Process Sanad (Narrator Chains) if rejal data exists
                        sanad_list = []
                        if rejal_data is not None and isinstance(rejal_data, dict):
                            data = rejal_data.get("data", {})
                            if isinstance(data, dict):
                                sanad_list = data.get("sanadList", [])
                        
                        print(f"Found {len(sanad_list)} sanad entries")
                        
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
                                        narrator_id,  # Foreign key to narrator
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
                                            patronymic
                                        ])
                                        
                                        # Mark this narrator as processed for additional details
                                        processed_narrator_details.add((narrator_id, ravi_id))
                                        
                                        print(f"Added details for narrator: {narrator_name}")
                                        
                                        # Extract death records
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
                                                summary
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
                        with open(os.path.join(csv_folder, skipped_files_log), 'a', encoding='utf-8') as skipped_file:
                            skipped_file.write(f"{filename},No valid data found,{datetime.now().isoformat()}\n")
                    
                    if has_valid_data:
                        print(f"Successfully processed Hadith ID: {hadith_id}")
                
                except Exception as e:
                    logging.error(f"Error processing file {filename}: {str(e)}")
                    print(f"⚠️ Error processing file {filename}: {str(e)}")
                    traceback.print_exc()
                    with open(os.path.join(csv_folder, skipped_files_log), 'a', encoding='utf-8') as skipped_file:
                        skipped_file.write(f"{filename},Processing error: {str(e).replace(',', ';')},{datetime.now().isoformat()}\n")
                    # Continue with the next file instead of stopping
                    continue

            print(f"\n✅ Processed {successful_entries} out of {len(json_files)} hadith entries successfully.")
            
            # Print info about skipped files log
            if os.path.exists(os.path.join(csv_folder, skipped_files_log)):
                with open(os.path.join(csv_folder, skipped_files_log), 'r', encoding='utf-8') as skipped_file:
                    skipped_count = len(skipped_file.readlines()) - 1  # Subtract header line
                print(f"\n⚠️ {skipped_count} files were skipped or failed. Check {skipped_files_log} for details.")
                print(f"Skipped files log saved to: {os.path.join(csv_folder, skipped_files_log)}")
            
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
    print(f"Skipped files log saved to: {os.path.join(csv_folder, skipped_files_log)}")
    return 0

# Execute main function
if __name__ == "__main__":
    sys.exit(main())

