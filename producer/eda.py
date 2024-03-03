import pandas as pd
import numpy as np
import re
import math

# Load flights_germany_uncleaned.csv into a pandas dataframe 
df = pd.read_csv("flights_germany.csv")

# Find how many departure_city and arrival_city have nan values
print("Number of nan values in departure_city: ", df["departure_city"].isna().sum())
print("Number of nan values in arrival_city: ", df["arrival_city"].isna().sum())