# -*- coding: utf-8 -*-
"""
Created on Mon Apr 01 12:11:03 2022

@author: Dane
"""

#importing files
import pandas as pd
import psycopg2
import datetime

#reading in the files
coviddeaths = pd.read_csv(r'C:\Users\Dane\Desktop\WriteToDatabase\CovidDeaths.csv')

#checking columns and dropping columns not needed for database analysis
coviddeaths.columns


#creating raw date with regex
coviddeaths['date_raw'] = ""
for i in range(0, len(coviddeaths)):
    coviddeaths['date_raw'].loc[i] = coviddeaths['submission_date'].loc[i].replace("/","")

#creating unique date_key for each row
coviddeaths['date_key'] = coviddeaths['date_raw'] + coviddeaths['state']


#making sure data types match database insert
coviddeaths = coviddeaths.astype({"date_key": str})
coviddeaths = coviddeaths.astype({"submission_date": str})
coviddeaths = coviddeaths.astype({"tot_death": float})
coviddeaths = coviddeaths.astype({"new_death": float})
coviddeaths = coviddeaths.astype({"state": str})
coviddeaths = coviddeaths.astype({"date_raw": str})


try:
    connection = psycopg2.connect(user="postgres",
                                  password="1988",
                                  host="localhost",
                                  port="5432",
                                  database="CDC_Data")
    cursor = connection.cursor()


    postgres_insert_query = """ INSERT INTO Covid_Deaths_Per_State 
    (date,
     state, 
     total_deaths,
     new_deaths,
     date_raw, 
     date_key) 
    
    VALUES (%s,
            %s,
            %s,
            %s,
            %s,
            %s)"""
    

    
    for i in range(0,len(coviddeaths)):
        record_to_insert = (coviddeaths.loc[i])  
        print(coviddeaths.loc[i])
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