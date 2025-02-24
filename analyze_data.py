import pandas as pd

# Load the CSV file
df = pd.read_csv("H1B_Visa_Sponsors.csv")

# Display first few rows
print(df.head())

# Convert 'Average Salary' column to numeric (removing $ and commas)
df["Average Salary"] = df["Average Salary"].replace(r'[\$,]', '', regex=True).astype(float)


# Summary statistics
print("\nSummary Statistics:")
print(df.describe())

# Save cleaned data
df.to_csv("H1B_Visa_Sponsors_Cleaned.csv", index=False)
print("\n✅ Cleaned data saved to H1B_Visa_Sponsors_Cleaned.csv")

import matplotlib.pyplot as plt
import seaborn as sns

# Load the cleaned data
df = pd.read_csv("H1B_Visa_Sponsors_Cleaned.csv")

# Plot top 5 employers by salary
plt.figure(figsize=(10, 6))
sns.barplot(x="Average Salary", y="H1B Visa Sponsor (Employer)", data=df.head(5), palette="viridis")
plt.xlabel("Average Salary ($)")
plt.ylabel("H1B Visa Sponsor")
plt.title("Top 5 H1B Visa Sponsors by Salary")
plt.show()


import pandas as pd

# Load the cleaned data
df = pd.read_csv("H1B_Visa_Sponsors_Cleaned.csv")

# Get row count
print(f"Total number of records: {len(df)}")
