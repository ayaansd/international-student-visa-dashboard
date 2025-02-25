from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
import time

# URLs to scrape
URLS = {
    "top_200_employers": "https://www.myvisajobs.com/reports/h1b/",
    "job_titles": "https://www.myvisajobs.com/reports/h1b/job-title/",
    "occupations": "https://www.myvisajobs.com/reports/h1b/occupation/",
    "locations": "https://www.myvisajobs.com/reports/h1b/location/",
    "application_status": "https://www.myvisajobs.com/reports/h1b/application-status/",
}

def setup_driver():
    """Set up Selenium WebDriver with options."""
    chrome_options = Options()
    chrome_options.add_argument("--headless")  # Run in the background
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("start-maximized")
    chrome_options.add_argument("enable-automation")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    return driver

def fetch_table_data(url, driver):
    """Scrapes MyVisaJobs data using Selenium with better table detection."""
    print(f"🔄 Fetching data from: {url}")
    driver.get(url)
    
    try:
        # Wait for table to appear
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CLASS_NAME, "tbl"))
        )
        
        time.sleep(5)  # Additional delay to allow JavaScript to fully load
        
        # Locate the table
        table = driver.find_element(By.CLASS_NAME, "tbl")
        rows = table.find_elements(By.TAG_NAME, "tr")

        data = []
        headers = [th.text.strip() for th in rows[0].find_elements(By.TAG_NAME, "th")]

        for row in rows[1:]:  # Skip header row
            cols = row.find_elements(By.TAG_NAME, "td")
            if len(cols) == len(headers):
                row_data = [col.text.strip().replace(",", "").replace("$", "") for col in cols]
                data.append(row_data)

        df = pd.DataFrame(data, columns=headers)
        return df

    except Exception as e:
        print(f"❌ Error extracting data from {url}: {e}")
        return None

if __name__ == "__main__":
    driver = setup_driver()

    for category, url in URLS.items():
        df = fetch_table_data(url, driver)
        if df is not None and not df.empty:
            print(f"\n📊 Extracted Data for {category} (First 5 Rows):")
            print(df.head())
        else:
            print(f"⚠️ No data extracted for {category}. Check if the page structure has changed.")

    driver.quit()
