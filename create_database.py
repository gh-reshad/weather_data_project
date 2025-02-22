from dotenv import load_dotenv
import os
import snowflake.connector


#Create A Snowflake Connection
load_dotenv("credintials.env")

conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT")
)

#create a cursor object
cur = conn.cursor()

#Create a Database and Schema
cur.execute("CREATE DATABASE IF NOT EXISTS WEATHER_DB")
cur.execute("CREATE SCHEMA IF NOT EXISTS WEATHER_DB.PUBLIC")

print("Databese and Schema Created Successfully")

#create a table
cur.execute("""
            CREATE TABLE IF NOT EXISTS WEATHER_DB.PUBLIC.weather_data(
            date DATE,
            station VARCHAR(50),
            precipitation FLOAT)

""")

print("Table Created Successfully")

'''
#close the connection
cur.close()
conn.close()'''