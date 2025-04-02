import time
import random
import json
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

# Anti-ban measures
def random_delay():
    time.sleep(random.uniform(2, 6))  # Random delay between 2-6 seconds

def setup_driver():
    options = Options()
    options.add_argument("--headless")  # Run in headless mode
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument(f'user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{random.randint(90, 120)}.0.{random.randint(1000, 9999)}.0 Safari/537.36')
    
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")  # Hide Selenium
    return driver

def scrape_hadith():
    url = "https://hadith.inoor.ir/fa/hadithlist?pagenumber=817&pagesize=60&sortcolumn=default&sortdirection=asc&searchtype=and&infeild=all&isgroup=0&isfulltext=0&iserab=1&pagesizegrouping=10&flexibleforstem=1&flexibleforletter=1&flexibleforroot=0&searchin=hadith"
    driver = setup_driver()
    driver.get(url)
    random_delay()
    
    data = []
    page = 817
    
    while True:
        print(f"Scraping Page: {page}")
        hadith_elements = driver.find_elements(By.CLASS_NAME, "ng-tns-c264-15")
        hadith_ids = [el.text.strip() for el in hadith_elements if el.text.strip().isdigit()]
        
        for hadith_id in hadith_ids:
            data.append({"page": page, "hadith_id": hadith_id})
            print(f"Page {page}, Hadith ID: {hadith_id}")
            
        # Save to file
        with open("hadith_data.json", "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        
        # Try to click next button
        try:
            next_button = driver.find_element(By.CLASS_NAME, "hadith-keyboard-arrow-left")
            ActionChains(driver).move_to_element(next_button).click().perform()
            random_delay()
            page += 1
        except:
            print("No more pages to scrape.")
            break
    
    driver.quit()

if __name__ == "__main__":
    scrape_hadith()