import pandas as pd
import numpy as np
import re
import math

# Load flights_germany_uncleaned.csv into a pandas dataframe 
df_flights = pd.read_csv("flights_germany_uncleaned.csv")

# Drop nan values from df
df_flights = df_flights.dropna()

# Assign key to each row
df_flights["key"] = df_flights.index

# Remove trailing whitespace from the departure_city and arrival_city columns
df_flights["departure_city"] = df_flights["departure_city"].str.strip()
df_flights["arrival_city"] = df_flights["arrival_city"].str.strip()

# Drop the price (€) column and rename the price column to price
df_flights["price"] = df_flights["price (€)"]
df_flights = df_flights.drop("price (€)", axis=1)

# Define a function to convert different time formats to 24-hour format
def convert_to_24hr(time_str):
    if isinstance(time_str, float):  # Check if time_str is a float
        return np.nan  # Return NaN if it's a float
    elif 'Uhr' in str(time_str):  # If it contains 'Uhr', it's in 24-hour format
        return str(time_str).split()[0]
    else:  # If it contains 'am' or 'pm', parse and convert to 24-hour format
        return pd.to_datetime(time_str).strftime('%H:%M')

# Apply the convert_to_24hr function to the departure_time and arrival_time columns
df_flights["departure_time"] = df_flights["departure_time"].apply(convert_to_24hr)
df_flights["arrival_time"] = df_flights["arrival_time"].apply(convert_to_24hr)

# Combine departure_date and departure_time into a single column as strings
df_flights["departure"] = df_flights["departure_date"] + " " + df_flights["departure_time"]
df_flights.drop("departure_time", axis=1, inplace=True)

# Combine arrival_date and arrival_time into a single column as strings
df_flights["arrival"] = df_flights["departure_date"] + " " + df_flights["arrival_time"]
df_flights.drop("arrival_time", axis=1, inplace=True)

df_flights.drop("departure_date", axis=1, inplace=True)

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
    
df_flights["stops"] = df_flights["stops"].apply(parse_stops)

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

df_flights["departure_date_distance"] = df_flights["departure_date_distance"].apply(convert_to_weeks)

print(df_flights.head(5))

# Convert 18.10.2019 to 2019-10-18 for scrape_date
df_flights["scrape_date"] = pd.to_datetime(df_flights["scrape_date"], format="%d.%m.%Y")

# Convert to 2024-03-02 14:30 for departure and arrival
df_flights["departure"] = pd.to_datetime(df_flights["departure"], format="%d.%m.%Y %H:%M")
df_flights["arrival"] = pd.to_datetime(df_flights["arrival"], format="%d.%m.%Y %H:%M")


# Extract unique values from the departure_city and arrival_city columns combined
unique_cities = pd.concat([df_flights["departure_city"], df_flights["arrival_city"]]).unique()

# Print the unique cities
print(unique_cities)


# Function to split city code and city name
def split_city(city):
    if isinstance(city, float):
        return np.nan, np.nan
    code, name = str(city).split(' ', 1)
    return code, name

# Split departure_city into departure_code and departure_name
df_flights[['departure_code', 'departure_name']] = df_flights['departure_city'].apply(split_city).apply(pd.Series)

# Split arrival_city into arrival_code and arrival_name
df_flights[['arrival_code', 'arrival_name']] = df_flights['arrival_city'].apply(split_city).apply(pd.Series)

# Extracting city names into another DataFrame
df_airports = pd.DataFrame({
    'short': df_flights['departure_code'].append(df_flights['arrival_code']).unique(),
    'long': df_flights['departure_name'].append(df_flights['arrival_name']).unique()
})

print("-----------------")
print(df_airports)

# Save city names to a file
df_airports.to_csv('../sample_data/airports.csv', index=False)

print("City names extracted and saved to 'airports.csv'")

# Drop the departure_city, arrival_city, departure_name and arrival_name columns
df_flights = df_flights.drop(['departure_city', 'arrival_city', 'departure_name', 'arrival_name'], axis=1)

# Split df_flights n dataframes consisting of rows with same scrape_date.
# Subsequently, shuffle each dataframe, and concatenate shuffled groups back into a single dataframe
df_flights = pd.concat([group.sample(frac=1) for _, group in df_flights.groupby('scrape_date')])

print(df_flights.head(5))

# Save the cleaned dataframe to a new csv file
df_flights.to_csv('../sample_data/flights_germany.csv', index=False)
