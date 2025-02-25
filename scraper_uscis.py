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

# Target URL for the 2025 H-1B Visa Report
URL = "https://www.myvisajobs.com/reports/h1b/"

# Headers to simulate a browser request (prevent blocking)
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0"
}

def fetch_h1b_data():
    """Scrapes the latest H-1B Visa Sponsorship data from MyVisaJobs."""
    print(f"🔄 Fetching data from: {URL}")

    response = requests.get(URL, headers=HEADERS)
    if response.status_code != 200:
        print(f"❌ Failed to fetch page, Status Code: {response.status_code}")
        return None

    soup = BeautifulSoup(response.text, "html.parser")

    # Find the correct table
    table = soup.find("table")
    if not table:
        print("❌ No data table found on the page!")
        return None

    print("✅ Table Found! Extracting data...")

    data = []
    headers = ["Rank", "Employer", "Number of LCA", "Average Salary"]

    # Extract rows
    for row in table.find_all("tr")[1:]:  # Skip header row
        cols = row.find_all("td")
        if len(cols) < 4:
            continue  # Skip rows that don't match the expected format

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

    # Convert to DataFrame
    df = pd.DataFrame(data, columns=headers)

    # Print sample extracted data
    print("\n📊 Extracted Data (First 5 Rows):")
    print(df.head())

    return df

def save_to_postgres(df):
    """Stores the H-1B Visa data into a PostgreSQL database."""
    if df is None or df.empty:
        print("❌ No data to save. Skipping database update.")
        return

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

        print("✅ Data successfully updated in PostgreSQL!")

    except Exception as e:
        print(f"❌ Database Error: {e}")

if __name__ == "__main__":
    df = fetch_h1b_data()
    save_to_postgres(df)
