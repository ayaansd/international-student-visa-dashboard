import os
import pandas as pd
import psycopg2
from glob import glob

# PostgreSQL Connection Parameters
DB_PARAMS = {
    "dbname": "visa_tracker2",
    "user": "postgres",
    "password": "postgres",
    "host": "localhost",
    "port": "5432",
}

# Directory where CSV files are stored
CSV_DIRECTORY = r"C:\Users\Syed\Downloads\h1b_data"

# List of CSV files (USCIS and Bloomberg)
uscis_files = glob(os.path.join(CSV_DIRECTORY, "h1b_datahubexport-*.csv"))  # All USCIS CSVs
bloomberg_file = os.path.join(CSV_DIRECTORY, "TRK_13139_FY2024_single_reg.csv")  # Bloomberg 2024


def process_uscis_data(file_path):
    """Reads and processes USCIS H1B data (2009-2023) with flexible column handling."""
    df = pd.read_csv(file_path, dtype=str)  # Read as string to handle missing values
    df = df.rename(columns={
        "Fiscal Year": "fiscal_year",
        "Employer": "employer_name",
        "State": "state",
        "City": "city",
        "ZIP": "zip_code",
    })

    # Identify correct column names for approvals
    approval_cols = [col for col in df.columns if "Initial Approval" in col]
    denial_cols = [col for col in df.columns if "Initial Denial" in col]

    initial_approval_col = approval_cols[0] if approval_cols else None
    initial_denial_col = denial_cols[0] if denial_cols else None

    if initial_approval_col and initial_denial_col:
        df[initial_approval_col] = df[initial_approval_col].str.replace(",", "").astype(float).fillna(0).astype(int)
        df[initial_denial_col] = df[initial_denial_col].str.replace(",", "").astype(float).fillna(0).astype(int)

        df["approval_status"] = df.apply(
            lambda row: "Approved" if row[initial_approval_col] > 0 else "Denied", axis=1
        )
    else:
        print(f"⚠️ Missing approval/denial columns in {file_path}, setting status as 'Unknown'.")
        df["approval_status"] = "Unknown"  # Fallback if missing

    return df[["fiscal_year", "employer_name", "state", "city", "zip_code", "approval_status"]]


def process_bloomberg_data(file_path):
    """Reads and processes Bloomberg H1B data (2024)."""
    df = pd.read_csv(file_path, dtype=str)  # Read as string

    # Ensure columns exist
    if "lottery_year" not in df.columns or "status_type" not in df.columns:
        print(f"⚠️ Missing required columns in {file_path}. Skipping processing.")
        return pd.DataFrame()

    df = df.rename(columns={
        "employer_name": "employer_name",
        "state": "state",
        "city": "city",
        "zip": "zip_code",
        "status_type": "approval_status",
    })

    # Convert lottery status to match USCIS dataset (No 'Denied' cases in Bloomberg data)
    df["approval_status"] = df["approval_status"].map({
        "ELIGIBLE": "Approved",
        "SELECTED": "Approved"
    }).fillna("Unknown")

    df["fiscal_year"] = "2024"  # Set fixed year for Bloomberg data

    return df[["fiscal_year", "employer_name", "state", "city", "zip_code", "approval_status"]]


def save_to_postgres(df):
    """Stores the merged H1B Visa data into PostgreSQL."""
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        cursor = conn.cursor()

        # Create table if not exists
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS h1b_visa_data (
            id SERIAL PRIMARY KEY,
            fiscal_year INT,
            employer_name TEXT,
            state TEXT,
            city TEXT,
            zip_code TEXT,
            approval_status TEXT
        )
        """)

        # Insert data
        for _, row in df.iterrows():
            cursor.execute("""
            INSERT INTO h1b_visa_data (fiscal_year, employer_name, state, city, zip_code, approval_status)
            VALUES (%s, %s, %s, %s, %s, %s)
            """, tuple(row))

        conn.commit()
        cursor.close()
        conn.close()

        print("✅ Data successfully saved to PostgreSQL!")

    except Exception as e:
        print(f"❌ Database Error: {e}")


def main():
    """Main function to process and save H1B data."""
    all_data = []

    # Process USCIS historical data
    for file in uscis_files:
        print(f"📂 Processing: {file}")
        df = process_uscis_data(file)
        all_data.append(df)

    # Process Bloomberg 2024 data
    print(f"📂 Processing: {bloomberg_file}")
    df_bloomberg = process_bloomberg_data(bloomberg_file)
    all_data.append(df_bloomberg)

    # Merge all data
    final_df = pd.concat(all_data, ignore_index=True)

    # Save to PostgreSQL
    save_to_postgres(final_df)

    print("✅ H1B Visa data processing complete!")


if __name__ == "__main__":
    main()
