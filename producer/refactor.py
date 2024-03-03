import pandas as pd
import numpy as np
import re
import math

# Load flights_germany_uncleaned.csv into a pandas dataframe 
df = pd.read_csv("flights_germany_uncleaned.csv")

# Assign key to each row
df["key"] = df.index

# Remove trailing whitespace from the departure_city and arrival_city columns
df["departure_city"] = df["departure_city"].str.strip()
df["arrival_city"] = df["arrival_city"].str.strip()

# Drop the price (€) column and rename the price column to price
df["price"] = df["price (€)"]
df = df.drop("price (€)", axis=1)

# Define a function to convert different time formats to 24-hour format
def convert_to_24hr(time_str):
    if isinstance(time_str, float):  # Check if time_str is a float
        return np.nan  # Return NaN if it's a float
    elif 'Uhr' in str(time_str):  # If it contains 'Uhr', it's in 24-hour format
        return str(time_str).split()[0]
    else:  # If it contains 'am' or 'pm', parse and convert to 24-hour format
        return pd.to_datetime(time_str).strftime('%H:%M')

# Apply the convert_to_24hr function to the departure_time and arrival_time columns
df["departure_time"] = df["departure_time"].apply(convert_to_24hr)
df["arrival_time"] = df["arrival_time"].apply(convert_to_24hr)

# Combine departure_date and departure_time into a single column as strings
df["departure"] = df["departure_date"] + " " + df["departure_time"]
df.drop("departure_time", axis=1, inplace=True)

# Combine arrival_date and arrival_time into a single column as strings
df["arrival"] = df["departure_date"] + " " + df["arrival_time"]
df.drop("arrival_time", axis=1, inplace=True)

df.drop("departure_date", axis=1, inplace=True)

# Parse stops
def parse_stops(stops):
    if pd.isnull(stops):
        return np.nan
    elif stops.lower() == 'direct':
        return 0  # Direct flights have 0 stops
    elif 'stop' in stops.lower():
        stops_count = re.findall(r'\d+', stops)  # Extract all numbers from the string
        if stops_count:
            return stops_count[0]  # Convert the first number found to an integer
        else:
            return np.nan
    else:
        return np.nan  # If the format is not recognized, return NaN
    
df["stops"] = df["stops"].apply(parse_stops)

# Apply the parse_stops function to the stops column
def convert_to_weeks(duration):
    if isinstance(duration, float) and math.isnan(duration):
        return np.nan
    elif 'week' in duration:
        return duration.split()[0]
    elif 'month' in duration:
        return str(int(duration.split()[0]) * 4)
    else:
        raise ValueError("Unsupported duration format")

df["departure_date_distance"] = df["departure_date_distance"].apply(convert_to_weeks)

print(df.head(5))

# Convert 18.10.2019 to 2019-10-18 for scrape_date
df["scrape_date"] = pd.to_datetime(df["scrape_date"], format="%d.%m.%Y")

# Convert to 2024-03-02 14:30 for departure and arrival
df["departure"] = pd.to_datetime(df["departure"], format="%d.%m.%Y %H:%M")
df["arrival"] = pd.to_datetime(df["arrival"], format="%d.%m.%Y %H:%M")

# Write  out flights_germany.csv
df.to_csv("flights_germany.csv", index=False)