# -*- coding: utf-8 -*-
"""
Created on Sun Apr 01 10:09:37 2022

@author: Dane
"""

#importing libraries
import pandas as pd
import psycopg2

#Reading in data
county_census = pd.read_csv(r'C:\Users\Dane\Desktop\WriteToDatabase\countyPopulation.csv')

county_census.columns
#matching data types for insert
county_census = county_census.astype({"State": str})
county_census = county_census.astype({"POPESTIMATE2020": float})
county_census = county_census.astype({"POPESTIMATE2021": float})
county_census = county_census.astype({"DEATHS2020": float})
county_census = county_census.astype({"DEATHS2020": float})


try:
    connection = psycopg2.connect(user="postgres",
                                  password="1988",
                                  host="localhost",
                                  port="5432",
                                  database="CDC_Data")
    cursor = connection.cursor()

    postgres_insert_query = """ INSERT INTO county_Census  
    (county, pop_2020, pop_2021, death_2020, death_2021) 
    VALUES (%s,%s,%s,%s,%s)"""

    for i in range(0, len(county_census)):
        record_to_insert = (county_census.loc[i])  
        cursor.execute(postgres_insert_query, record_to_insert)
        connection.commit()
        count = cursor.rowcount
        print("Record inserted successfully into mobile table")
    

except (Exception, psycopg2.Error) as error:
    print("Failed to insert record into mobile table", error)

finally:
    # closing database connection.
    if connection:
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")