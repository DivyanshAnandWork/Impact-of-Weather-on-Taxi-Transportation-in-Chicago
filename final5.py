import pyarrow.hdfs as hdfs
import pandas as pd
from io import StringIO

# Specify the HDFS file paths
noaa_gsod_file_path = "div/input/noaaGsod.csv"
taxi_trip_file_path = "div/input/taxitrip.csv"

# Connect to HDFS
fs = hdfs.connect()

# Read the first CSV file (noaa_gsod.csv) from HDFS
with fs.open(noaa_gsod_file_path, 'rb') as file:
    noaa_gsod_csv_content = file.read().decode('utf-8')

# Read the second CSV file (taxi_trip.csv) from HDFS
with fs.open(taxi_trip_file_path, 'rb') as file:
    taxi_trip_csv_content = file.read().decode('utf-8')

# Use pandas to read CSV content into DataFrames
noaa_gsod_df = pd.read_csv(StringIO(noaa_gsod_csv_content))
taxi_trip_df = pd.read_csv(StringIO(taxi_trip_csv_content))

# Now you can work with the DataFrames as needed
print("NOAA GSOD DataFrame:")
print(noaa_gsod_df.head())

print("\nTaxi Trip DataFrame:")
print(taxi_trip_df.head())

# Check for null values in the DataFrames
print("Null values in NOAA GSOD DataFrame:")
print(noaa_gsod_df.isnull().sum())

# Drop specific columns with null values
columns_to_drop = ['flag_max', 'flag_min', 'flag_prcp']
noaa_gsod_df = noaa_gsod_df.drop(columns=columns_to_drop)

# Display null values count after dropping columns
print("\nNull values after dropping columns:")
print(noaa_gsod_df.isnull().sum())

# Disply null values count before removing rows
print("\nNull values in Taxi Trip DataFrame:")
print(taxi_trip_df.isnull().sum())

# No null values found

# print("\nNo null values found!!")

# Check for duplicate records in the noaa_gsod_df DataFrame
print("Duplicate records in NOAA GSOD DataFrame:")
duplicate_count_noaa_gsod = noaa_gsod_df.duplicated().sum()
print("Number of duplicate rows:", duplicate_count_noaa_gsod)

# Check for duplicate records in the taxi_trip_df DataFrame
print("\nDuplicate records in Taxi Trip DataFrame:")
duplicate_count_taxi_trip = taxi_trip_df.duplicated().sum()
print("Number of duplicate rows:", duplicate_count_taxi_trip)

print("\n Transformation: Joing the dataframes on the basis of date")

# Convert 'date' columns to datetime objects
noaa_gsod_df['date'] = pd.to_datetime(noaa_gsod_df['date'])
taxi_trip_df['date'] = pd.to_datetime(taxi_trip_df['date'])

# Merge DataFrames on the 'year' column
merged_df = pd.merge(noaa_gsod_df, taxi_trip_df, on='date', how='inner')

# Display the merged DataFrame
print(merged_df.columns)
print(merged_df.shape)
################# KPI 1################

print("Map reduce process started..")
# Mapper Function
def mapper(record):
    date = record['date']
    #location = (record['stn'], record['wban'])  # Using station information as location
    weather_conditions = {
        'temp': record['temp'],
        'dewp': record['dewp'],
        'slp': record['slp'],
		'prcp': record['prcp'],
		'fog': record['fog'],
		'snow_ice_pellets': record['snow_ice_pellets'],
        'hail': record['hail'],
		'thunder': record['thunder'],
		'tornado_funnel_cloud': record['tornado_funnel_cloud'],
    }
    taxi_demand = {
	'bad_condition': int(record['fog']) | int(record['snow_ice_pellets']) | int(record['hail']) | int(record['thunder']) | int(record['tornado_funnel_cloud']),
        'total_tips': record['total_tips']
    }

    emit_key = (str(date), taxi_demand['bad_condition'])
    emit_value = {'weather_conditions': weather_conditions, 'taxi_demand': taxi_demand}

    return emit_key, emit_value


# Reducer Function
def reducer(key, values):
    total_taxi_demand = 0
    total_records = 0

    for value in values:
        total_taxi_demand += value['taxi_demand']['total_tips']
        total_records += 1

    average_taxi_demand = total_taxi_demand / total_records if total_records > 0 else 0

    yield key, {'average_taxi_demand': average_taxi_demand}

# Convert DataFrame to list of dictionaries
input_data = merged_df.to_dict(orient='records')


# MapReduce Execution
mapper_output = [mapper(record) for record in input_data]

print(type(mapper_output[0]))

# Group data by key (date, location)
grouped_data = {}
for key, value in mapper_output:
    if key not in grouped_data:
        grouped_data[key] = []
    grouped_data[key].append(value)

# Run reducer on each group
reducer_output = [result for key, values in grouped_data.items() for result in reducer(key, values)]

# Output Results
transformed_output = [
    {
        'timestamp': key[0],
        'bad_condition': key[1],
        'average_taxi_tips': values['average_taxi_demand']
    }
    for key, values in reducer_output
]

df = pd.DataFrame(transformed_output)
print(df.head(100))
df.to_csv('kp5.csv', index=False)
