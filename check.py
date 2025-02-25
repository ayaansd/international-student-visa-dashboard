import requests
from bs4 import BeautifulSoup
import pandas as pd
import psycopg2
import time

# PostgreSQL Connection Parameters
DB_PARAMS = {
    "dbname": "visa_tracker",
    "user": "postgres",
    "password": "postgres",
    "host": "localhost",
    "port": "5432",
}

# Target URL for H-1B Visa Employer Data
BASE_URL = "https://www.myvisajobs.com/Reports/"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0"
}

def fetch_h1b_data():
    """Scrapes the latest H-1B Visa Sponsorship data from MyVisaJobs."""
    print("🔄 Fetching H-1B Visa Employer Data...")
    
    response = requests.get(BASE_URL, headers=HEADERS)
    if response.status_code != 200:
        print("❌ Failed to fetch MyVisaJobs page.")
        return None

    soup = BeautifulSoup(response.text, "html.parser")

    # Find the "Top 200 H-1B Employers" link
    table_link = soup.find("a", string="Top 200 H-1B Employers")
    if not table_link:
        print("❌ 'Top 200 H-1B Employers' link not found. Check the website manually.")
        return None

    # Extract table link and navigate to it
    table_url = "https://www.myvisajobs.com" + table_link["href"]
    print(f"🔗 Navigating to: {table_url}")

    data = []
    page = 1
    while True:
        page_url = f"{table_url}?P={page}"
        print(f"📄 Scraping page {page}...")

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

    df = pd.DataFrame(data, columns=["Rank", "Employer", "Number of LCA", "Average Salary"])
    return df

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
    df = fetch_h1b_data()
    if df is not None:
        save_to_postgres(df)
