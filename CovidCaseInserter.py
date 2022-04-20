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
covidcases = pd.read_csv(r'C:\Users\Dane\Desktop\WriteToDatabase\CovidCases.csv')

#checking columns and dropping columns not needed for database analysis
covidcases.columns
covidcases = covidcases.drop(columns=['conf_cases',
                         'prob_cases',
                         'pnew_case', 'tot_death', 'conf_death', 'prob_death',
       'new_death', 'pnew_death', 'created_at', 'consent_cases',
       'consent_deaths'])

#creating raw date with regex
covidcases['date_raw'] = ""
for i in range(0, len(covidcases)):
    covidcases['date_raw'].loc[i] = covidcases['submission_date'].loc[i].replace("/","")

#creating unique date_key for each row
covidcases['date_key'] = covidcases['date_raw'] + covidcases['state']


#making sure data types match database insert
covidcases = covidcases.astype({"date_key": str})
covidcases = covidcases.astype({"date_raw": str})
covidcases = covidcases.astype({"submission_date": str})
covidcases = covidcases.astype({"tot_cases": float})
covidcases = covidcases.astype({"new_case": float})
covidcases = covidcases.astype({"state": str})
covidcases = covidcases.astype({"date_raw": str})
#covidcases

try:
    connection = psycopg2.connect(user="postgres",
                                  password="1988",
                                  host="localhost",
                                  port="5432",
                                  database="CDC_Data")
    cursor = connection.cursor()


    postgres_insert_query = """ INSERT INTO Covid_Cases_Per_State 
    (date,
     state, 
     cases,
     new_cases,
     date_raw, 
     date_key) 
    
    VALUES (%s,
            %s,
            %s,
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
    
    for i in range(0,len(covidcases)):
        record_to_insert = (covidcases.loc[i])  
        print(covidcases.loc[i])
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