import pandas as pd

# Load the Bloomberg 2024 dataset
file_path = r"C:\Users\Syed\Downloads\h1b_data\TRK_13139_FY2024_single_reg.csv"
df_bloomberg = pd.read_csv(file_path, dtype=str, low_memory=False)

# Print column names
print("🧐 Column Names:")
print(df_bloomberg.columns.tolist())

# Print first few rows
print("\n📊 First 5 Rows:")
print(df_bloomberg.head())
