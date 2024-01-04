import pandas as pd
# Load flights_germany_uncleaned.csv into a pandas dataframe 
df = pd.read_csv("flights_germany_uncleaned.csv")

# Assign key to each row
df["key"] = df.index

# Write  out flights_germany.csv
df.to_csv("flights_germany.csv", index=False)