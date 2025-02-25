import requests
from bs4 import BeautifulSoup
import pandas as pd
import psycopg2
from datetime import datetime, timezone
import time

# PostgreSQL Connection Parameters
DB_PARAMS = {
    "dbname": "visa_tracker2",
    "user": "postgres",
    "password": "postgres",
    "host": "localhost",
    "port": "5432",
}

# Define URLs for each section
H1B_URLS = {
    "h1b_top_companies": "https://h1bdata.info/topcompanies.php",
    "h1b_top_jobs": "https://h1bdata.info/topjobs.php",
    "h1b_top_cities": "https://h1bdata.info/topcities.php",
    "h1b_highest_paid_companies": "https://h1bdata.info/highestpaidcompany.php",
    "h1b_highest_paid_jobs": "https://h1bdata.info/highestpaidjob.php",
    "h1b_highest_paid_cities": "https://h1bdata.info/highestpaidcity.php",
}

# Headers to mimic a browser request
HEADERS = {"User-Agent": "Mozilla/5.0"}

def fetch_h1b_data(section, url):
    """Scrapes H1BData.info for a specific section."""
    print(f"🔄 Fetching {section} data...")

    response = requests.get(url, headers=HEADERS)
    if response.status_code != 200:
        print(f"❌ Failed to fetch {section}! Status Code: {response.status_code}")
        return None

    soup = BeautifulSoup(response.text, "html.parser")
    table = soup.find("table")

    if not table:
        print(f"❌ No table found for {section}! Website structure might have changed.")
        return None

    print(f"✅ Table Found for {section}! Extracting data...")

    # Extract table headers
    headers = [th.text.strip().lower().replace(" ", "_") for th in table.find_all("th")]

    # Extract rows
    data = []
    for row in table.find_all("tr")[1:]:  # Skip header row
        cols = row.find_all("td")
        data.append([col.text.strip() for col in cols])

    # Convert to DataFrame
    df = pd.DataFrame(data, columns=headers)

    # Rename columns based on database schema
    rename_map = {
        "#": "rank",
        "#_of_h-1b_filings": "filings",
        "average_salary": "avg_salary",
        "city": "city_name",
        "latest_filings": "latest_filings"
    }
    df.rename(columns=rename_map, inplace=True)

    # Drop unnecessary columns
    df.drop(columns=["latest_filings"], errors="ignore", inplace=True)

    # Convert numeric columns safely
    if "filings" in df.columns:
        df["filings"] = df["filings"].str.replace(",", "").fillna("0").astype(int)

    if "avg_salary" in df.columns:
        df["avg_salary"] = df["avg_salary"].str.replace(r"[\$,]", "", regex=True).fillna("0").astype(float)

    # **✅ Fix: Store UTC timestamp correctly**
    df["last_updated"] = datetime.now(timezone.utc)

    print(f"\n📊 {section} Data - {df.shape[0]} rows extracted:")
    print(df.head())

    return df

def save_to_postgres(df, table_name):
    """Pushes the DataFrame to PostgreSQL with real-time updates."""
    if df is None or df.empty:
        print(f"❌ No data to save for {table_name}. Skipping database update.")
        return

    try:
        conn = psycopg2.connect(**DB_PARAMS)
        cursor = conn.cursor()

        # Define unique constraint column
        unique_column_mapping = {
            "h1b_top_companies": "company_name",
            "h1b_top_jobs": "job_title",
            "h1b_top_cities": "city_name",
            "h1b_highest_paid_companies": "company_name",
            "h1b_highest_paid_jobs": "job_title",
            "h1b_highest_paid_cities": "city_name",
        }
        unique_column = unique_column_mapping[table_name]

        # Expected columns
        column_mapping = {
            "h1b_top_companies": ["company_name", "filings", "avg_salary", "last_updated"],
            "h1b_top_jobs": ["job_title", "filings", "avg_salary", "last_updated"],
            "h1b_top_cities": ["city_name", "filings", "avg_salary", "last_updated"],
            "h1b_highest_paid_companies": ["company_name", "filings", "avg_salary", "last_updated"],
            "h1b_highest_paid_jobs": ["job_title", "filings", "avg_salary", "last_updated"],
            "h1b_highest_paid_cities": ["city_name", "filings", "avg_salary", "last_updated"],
        }
        expected_columns = column_mapping[table_name]

        df = df[expected_columns]

        # SQL Query for Insertion with Conflict Handling
        placeholders = ", ".join(["%s"] * len(expected_columns))
        columns_sql = ", ".join(expected_columns)

        for row in df.itertuples(index=False, name=None):
            cursor.execute(
                f"""
                INSERT INTO {table_name} ({columns_sql})
                VALUES ({placeholders})
                ON CONFLICT ({unique_column}) DO UPDATE
                SET filings = EXCLUDED.filings,
                    avg_salary = EXCLUDED.avg_salary,
                    last_updated = EXCLUDED.last_updated;
                """,
                row,
            )

        conn.commit()
        cursor.close()
        conn.close()

        print(f"✅ Real-time data updated in PostgreSQL for {table_name}!")

    except Exception as e:
        print(f"❌ Database Error for {table_name}: {e}")

if __name__ == "__main__":
    while True:
        print("\n🔄 Running Real-Time H1B Data Update...")
        for section, url in H1B_URLS.items():
            df = fetch_h1b_data(section, url)
            if df is not None:
                save_to_postgres(df, section)

        print("⏳ Sleeping for 10 minutes before next update...")
        time.sleep(600)  # Run every 10 minutes
