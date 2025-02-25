import pandas as pd

# Data extracted
data = [
    ["1", "Amazon.Com Services", "10,969", "$149,812"],
    ["2", "Cognizant Technology Solutions", "8,688", "$101,773"],
    ["3", "Ernst & Young", "8,674", "$143,378"],
    ["4", "Tata Consultancy Services", "8,120", "$105,529"],
    ["5", "Google", "7,649", "$178,184"],
    ["6", "Microsoft", "6,649", "$163,672"],
    ["7", "Infosys", "4,926", "$103,102"],
    ["8", "Meta Platforms", "4,566", "$199,944"],
    ["9", "Intel", "3,242", "$145,250"]
]

# Create DataFrame
df = pd.DataFrame(data, columns=["Rank", "H1B Visa Sponsor (Employer)", "Number of LCA", "Average Salary"])

# Save to CSV
df.to_csv("H1B_Visa_Sponsors.csv", index=False)

print("✅ Data saved to H1B_Visa_Sponsors.csv")
