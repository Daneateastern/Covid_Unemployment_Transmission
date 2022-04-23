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
covidcommunitylevels = pd.read_csv(r'C:\Users\Dane\Desktop\WriteToDatabase\State_Covid_Community_levels.csv')

covidcommunitylevels.columns
#matching data with inserts
covidcommunitylevels = covidcommunitylevels.astype({"state": str})
covidcommunitylevels = covidcommunitylevels.astype({"community_level": str})
covidcommunitylevels = covidcommunitylevels.astype({"date_raw": str})
    
covidcommunitylevels['date_key'] = covidcommunitylevels['date_raw'] + covidcommunitylevels['state']
covidcommunitylevels = covidcommunitylevels.astype({"date_raw": str})

covidcommunitylevels.columns

try:
    connection = psycopg2.connect(user="postgres",
                                  password="1988",
                                  host="localhost",
                                  port="5432",
                                  database="CDC_Data")
    cursor = connection.cursor()


    postgres_insert_query = """ INSERT INTO Community_Levels_per_state 
    (date_raw,
     state,
     covid_19_community_level,
     date_key) 
    
    VALUES (%s,
            %s,
            %s,
            %s)"""
    
    """
    record_to_insert = (covidcommunitylevels.loc[0])  
    cursor.execute(postgres_insert_query, record_to_insert)
    connection.commit()
    count = cursor.rowcount
    print("Record inserted successfully into mobile table")
    """
    
    for i in range(0,len(covidcommunitylevels)):
        record_to_insert = (covidcommunitylevels.loc[i])  
        print(covidcommunitylevels.loc[i])
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