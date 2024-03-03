import pandas as pd
import numpy as np
import re
import math

# Load flights_germany_uncleaned.csv into a pandas dataframe 
df = pd.read_csv("flights_germany.csv")

# Drop nan values from df
df = df.dropna()

# Extract unique values from the departure_city and arrival_city columns combined
unique_cities = pd.concat([df["departure_city"], df["arrival_city"]]).unique()

# Print the unique cities
print(unique_cities)


# Function to split city code and city name
def split_city(city):
    if isinstance(city, float):
        return np.nan, np.nan
    code, name = str(city).split(' ', 1)
    return code, name

# Split departure_city into departure_code and departure_name
df[['departure_code', 'departure_name']] = df['departure_city'].apply(split_city).apply(pd.Series)

# Split arrival_city into arrival_code and arrival_name
df[['arrival_code', 'arrival_name']] = df['arrival_city'].apply(split_city).apply(pd.Series)

# Extracting city names into another DataFrame
airports = pd.DataFrame({
    'short': df['departure_code'].append(df['arrival_code']).unique(),
    'long': df['departure_name'].append(df['arrival_name']).unique()
})

# Save city names to a file
airports.to_csv('airports.csv', index=False)

print("City names extracted and saved to 'airports.csv'")

# Drop the departure_city, arrival_city, departure_name and arrival_name columns
df = df.drop(['departure_city', 'arrival_city', 'departure_name', 'arrival_name'], axis=1)

# Save the cleaned dataframe to a new csv file
df.to_csv('flights_germany.csv', index=False)
