import requests
from bs4 import BeautifulSoup
import pandas as pd
import psycopg2
import time

# PostgreSQL Connection Parameters
DB_PARAMS = {
    "dbname": "visa_tracker2",
    "user": "postgres",
    "password": "postgres",
    "host": "localhost",
    "port": "5432",
}

# Direct URLs for H-1B Visa Data
H1B_URLS = [
    "https://www.myvisajobs.com/reports/h1b/",  # Top 200 H-1B Employers
    "https://www.myvisajobs.com/reports/h1b/job-title/",  # Employers by Job Title
    "https://www.myvisajobs.com/reports/h1b/occupation/",  # Employers by Occupation
    "https://www.myvisajobs.com/reports/h1b/location/",  # Employers by Location
    "https://www.myvisajobs.com/reports/h1b/application-status/"  # Employers by Application Status
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0"
}

MAX_PAGES = 20  # Limit the scraper to 100 pages

def fetch_h1b_data(url):
    """Scrapes H-1B Visa Sponsorship data from a given MyVisaJobs URL."""
    print(f"🔄 Fetching data from: {url}")

    data = []
    page = 1
    while page <= MAX_PAGES:  # Limit to 100 pages
        page_url = f"{url}?P={page}"
        print(f"📄 Scraping page {page}/{MAX_PAGES}...")

        response = requests.get(page_url, headers=HEADERS)
        soup = BeautifulSoup(response.text, "html.parser")

        # Find the table
        table = soup.find("table")
        if not table:
            print("❌ No more data available or unable to locate the table.")
            break

        # Extract rows
        for row in table.find_all("tr")[1:]:  # Skip header row
            cols = row.find_all("td")
            if len(cols) < 4:
                continue  # Skip if row does not have expected columns

            try:
                rank_text = cols[0].text.strip()
                employer = cols[1].text.strip()
                lca_count_text = cols[2].text.strip().replace(",", "")
                avg_salary_text = cols[3].text.strip().replace("$", "").replace(",", "")

                # Convert extracted values safely
                rank = int(rank_text) if rank_text.isdigit() else None
                lca_count = int(lca_count_text) if lca_count_text.isdigit() else None
                avg_salary = float(avg_salary_text) if avg_salary_text.replace(".", "").isdigit() else None

                if rank and employer and lca_count and avg_salary:
                    data.append([rank, employer, lca_count, avg_salary])

            except Exception as e:
                print(f"⚠️ Skipping row due to error: {e}")

        page += 1
        time.sleep(2)  # Delay to prevent blocking

    return pd.DataFrame(data, columns=["Rank", "Employer", "Number of LCA", "Average Salary"])

def save_to_postgres(df):
    """Stores the H-1B Visa data into PostgreSQL."""
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        cursor = conn.cursor()

        # Create Table if it doesn't exist
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS h1b_visa_sponsorships (
            id SERIAL PRIMARY KEY,
            rank INT UNIQUE,
            employer TEXT,
            lca_count INT,
            average_salary NUMERIC,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)

        # Insert or Update Data
        for _, row in df.iterrows():
            cursor.execute("""
            INSERT INTO h1b_visa_sponsorships (rank, employer, lca_count, average_salary, last_updated)
            VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (rank) DO UPDATE
            SET employer = EXCLUDED.employer,
                lca_count = EXCLUDED.lca_count,
                average_salary = EXCLUDED.average_salary,
                last_updated = CURRENT_TIMESTAMP;
            """, (row["Rank"], row["Employer"], row["Number of LCA"], row["Average Salary"]))

        conn.commit()
        cursor.close()
        conn.close()

        print("✅ H-1B Visa Data successfully saved to PostgreSQL!")

    except Exception as e:
        print(f"❌ Database Error: {e}")

if __name__ == "__main__":
    for url in H1B_URLS:
        df = fetch_h1b_data(url)
        if df is not None and not df.empty:
            save_to_postgres(df)
