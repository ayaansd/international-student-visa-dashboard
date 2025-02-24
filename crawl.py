import requests
from bs4 import BeautifulSoup
import psycopg2
import queue
import time
from urllib.parse import urljoin
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

# PostgreSQL Connection
DB_PARAMS = {
    "dbname": "visa_tracker",
    "user": "postgres",
    "password": "postgres",
    "host": "localhost",
    "port": "5432",
}

# Visa-related forums and discussion pages
SEED_LINKS = [
    "https://www.trackitt.com/usa-discussion-forums/h1b-visa",
    "https://www.trackitt.com/usa-discussion-forums/opt",
    "https://www.visagrader.com/discussions",
    "https://www.myvisajobs.com/Reports/"
]

# Function to set up Selenium WebDriver
def setup_driver():
    """Initialize Selenium WebDriver for JavaScript-heavy pages."""
    chrome_options = Options()
    chrome_options.add_argument("--headless")  # Run in the background
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled") 
    chrome_options.add_argument("--ignore-certificate-errors")  # 🔹 Fix SSL errors
    chrome_options.add_argument("--allow-running-insecure-content")

    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=chrome_options)

def fetch_discussions(seed_url):
    """Crawls a visa discussion forum page for relevant links and timestamps."""
    driver = setup_driver()
    driver.get(seed_url)
    time.sleep(5)  # Allow page to load
    
    discussions = []
    posts = driver.find_elements(By.CSS_SELECTOR, "div.thread-info")  # Adjust based on site structure

    for post in posts:
        try:
            title = post.find_element(By.CSS_SELECTOR, "a.thread-title").text.strip()
            link = "https://www.trackitt.com" + post.find_element(By.CSS_SELECTOR, "a.thread-title").get_attribute("href")
            timestamp = post.find_element(By.CSS_SELECTOR, "span.thread-date").text.strip()
            
            discussions.append((title, link, timestamp))
        except Exception as e:
            print(f"❌ Error extracting post: {e}")

    driver.quit()
    return discussions
print(f"🔍 Extracted {len(discussions)} discussions from {seed_url}")
print("🔍 Sample Data:", discussions[:5])  # Print first 5 extracted records

def save_to_postgres(discussions, source):
    """Stores extracted discussions into PostgreSQL."""
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        cursor = conn.cursor()

        # 🔹 Ensure table names are valid
        source = source.replace("-", "_").replace(".", "_")

        cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {source}_discussions (
            id SERIAL PRIMARY KEY,
            title TEXT,
            link TEXT UNIQUE,
            timestamp TEXT,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)

        for title, link, timestamp in discussions:
            cursor.execute(f"""
            INSERT INTO {source}_discussions (title, link, timestamp)
            VALUES (%s, %s, %s)
            ON CONFLICT (link) DO NOTHING
            """, (title, link, timestamp))

        conn.commit()
        cursor.close()
        conn.close()
        print(f"✅ {source.capitalize()} Discussions saved to PostgreSQL!")

    except Exception as e:
        print(f"❌ Database Error: {e}")

if __name__ == "__main__":
    for link in SEED_LINKS:
        print(f"🔄 Crawling: {link}")
        discussions = fetch_discussions(link)
        save_to_postgres(discussions, link.split("/")[-1].replace("-", "_").replace(".", "_"))
        print(f"✅ Completed crawling: {link}\n")
