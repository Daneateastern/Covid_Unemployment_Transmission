# -*- coding: utf-8 -*-
"""
Created on Sun Apr 01 21:01:46 2022

@author: Dane
"""

#importing libraries
import pandas as pd
import psycopg2
import datetime

#reading in data
rentIncrease = pd.read_excel(r'C:\Users\Dane\Desktop\WriteToDatabase\RentIncreaseByState.xlsx')

rentIncrease.columns
#matching data with inserts
rentIncrease = rentIncrease.astype({"State": str})
rentIncrease = rentIncrease.astype({"2022Increase": float})
rentIncrease = rentIncrease.astype({"2021Increase": float})
    

rentIncrease.columns

#rearranging the columns to match the ingest
dataToInsert = rentIncrease[["2021Increase","2022Increase","State"]]
rentIncrease = dataToInsert

try:
    connection = psycopg2.connect(user="postgres",
                                  password="1988",
                                  host="localhost",
                                  port="5432",
                                  database="CDC_Data")
    cursor = connection.cursor()


    postgres_insert_query = """ INSERT INTO Rent_Increases_Per_State 
    (
     increase_2021,
     increase_2022,
     state
     ) 
    
    VALUES (%s,
            %s,
            %s)"""
    
    
    for i in range(0,len(rentIncrease)):
        record_to_insert = (rentIncrease.loc[i])  
        print(rentIncrease.loc[i])
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