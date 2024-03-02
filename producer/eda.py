import pandas as pd

# Load dataframes from CSV file flight_germany.csv
df = pd.read_csv("flights_germany.csv")

# Find where departure time is between 23:00 and 00:00, compare as string
print(df[(df["departure_time"] > "23:00") & (df["departure_time"] < "00:00")])