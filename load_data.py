import pandas as pd
import requests
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
import create_database


#NOAA API details
NOAA_API_URL = "https://www.ncdc.noaa.gov/cdo-web/api/v2/data"
NOAA_API_TOKEN = "rwZalziazOwYJsiNhfBCTQqQvJgYHgzf"

#API request parameters
params = {
    "datasetid": "GHCND",
    "locationid": "FIPS:06",
    "startdate": "2020-01-01",
    "enddate": "2020-12-31",
    "limit": 1000,
    "datatypeid": "PRCP",
    "units": "metric",
}

headers = {"token": NOAA_API_TOKEN}
response = requests.get(NOAA_API_URL, headers=headers, params=params)

if response.status_code == 200:
    data = response.json()
    records = [(item["date"], item["station"], item["value"]) for item in data["results"]]

    #convert to DataFrame
    df = pd.DataFrame(records, columns=['date', 'station', 'precipitation'])

    #Save to CSV
    df.to_csv("weather_data.csv", index=False)
    print("Data saved locally as noaa_weather_data.csv")

else:
    print("Error fetching data:", response.status_code, response.text)

#load credintials
load_dotenv("db_info.env")

#create engine
engine = create_engine(
    f"snowflake://{os.getenv('SNOWFLAKE_USER')}:{os.getenv('SNOWFLAKE_PASSWORD')}"
    f"@{os.getenv('SNOWFLAKE_ACCOUNT')}/{os.getenv('SNOWFLAKE_DATABASE')}/{os.getenv('SNOWFLAKE_SCHEMA')}"
)

#load data to Snowflake
df.to_sql("weather_data", engine, if_exists="replace", index=False)

print("Data inserted into Snowflake!")