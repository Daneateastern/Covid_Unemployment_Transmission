# -*- coding: utf-8 -*-
"""
Created on Sun Apr 01 10:09:37 2022

@author: Dane
"""

#importing libraries
import pandas as pd
import psycopg2

#Reading in data
unemployment = pd.read_excel(r'C:\Users\Dane\Desktop\WriteToDatabase\UnemploymentClean.xlsx')

#dropping old data we don't need
unemployment = unemployment[unemployment['Year'] > 2018]

#matching data types for insert
unemployment = unemployment.astype({"State_Or_Area": str})
unemployment = unemployment.astype({"Year": str})
unemployment = unemployment.astype({"FIPS": str})
unemployment = unemployment.astype({"Month": str})
unemployment = unemployment.astype({"Unemployment_Total": float})
unemployment = unemployment.astype({"Unemployment_Rate": float})
unemployment['date_key'] = unemployment['FIPS'] + unemployment['Year'] + unemployment['Month'] + unemployment['State_Or_Area'] 
unemployment = unemployment.astype({"date_key": str})

#grabbing fips codes for only cities
city_unemployment = unemployment[unemployment['FIPS'].map(len) > 2]
city_unemployment = city_unemployment.reset_index(drop=True)


try:
    connection = psycopg2.connect(user="postgres",
                                  password="1988",
                                  host="localhost",
                                  port="5432",
                                  database="CDC_Data")
    cursor = connection.cursor()

    postgres_insert_query = """ INSERT INTO Unemployment_Rate_By_City  
    (FIPS, city, year, month, unemployment_total, unemployment_rate, date_key) 
    VALUES (%s,%s,%s,%s,%s,%s,%s)"""

    for i in range(0, len(city_unemployment)):
        record_to_insert = (city_unemployment.loc[i])  
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