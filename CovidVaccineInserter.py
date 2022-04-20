# -*- coding: utf-8 -*-
"""
Created on Mon Apr 01 12:44:56 2022

@author: Dane
"""

import pandas as pd
import psycopg2
import datetime
from random import seed
from random import randint

#Reading in the data
covidvaccination = pd.read_csv(r'C:\Users\Dane\Desktop\WriteToDatabase\CovidVaccinations.csv')

#Dropping columns not needed for analysis
covidvaccination.columns
covidvaccination = covidvaccination.drop(columns=['MMWR_week',
                         'Administered_Cumulative', 'Administered_7_Day_Rolling_Average',
       'Admin_Dose_1_Cumulative',
       'Admin_Dose_1_Day_Rolling_Average', 'Administered_Dose1_Pop_Pct',
       'date_type', 'Administered_daily_change_report',
       'Administered_daily_change_report_7dayroll', 'Series_Complete_Daily',
       'Series_Complete_Cumulative', 'Series_Complete_Day_Rolling_Average',
       'Series_Complete_Pop_Pct', 'Booster_Cumulative',
       'Booster_7_Day_Rolling_Average', 'Additional_Doses_Vax_Pct'])

#creating raw date and date key
covidvaccination['date_raw'] = ""
covidvaccination['date_key'] = ""
seed(1)
for i in range(0, len(covidvaccination)):
    covidvaccination['date_raw'].loc[i] = covidvaccination['Date'].loc[i].replace("/","")
    covidvaccination['date_key'].loc[i] = covidvaccination['date_raw'].loc[i] + covidvaccination['Location'].loc[i] + str(randint(0, 1000000))



#Matching types with database inserts
covidvaccination = covidvaccination.astype({"date_key": str})
covidvaccination = covidvaccination.astype({"date_raw": str})
covidvaccination = covidvaccination.astype({"Date": str})
covidvaccination = covidvaccination.astype({"Administered_Daily": str})
covidvaccination = covidvaccination.astype({"Booster_Daily": str})
covidvaccination = covidvaccination.astype({"Admin_Dose_1_Daily": str})
covidvaccination = covidvaccination.astype({"Location": str})

try:
    connection = psycopg2.connect(user="postgres",
                                  password="1988",
                                  host="localhost",
                                  port="5432",
                                  database="CDC_Data")
    cursor = connection.cursor()


    postgres_insert_query = """ INSERT INTO Covid_Vaccines_Per_State 
    (date,
     state, 
     daily_administered,
     daily_dose_1,
     daily_boosters,
     date_raw, 
     date_key) 
    
    VALUES (%s,
            %s,
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
    
    for i in range(0,len(covidvaccination)):
        record_to_insert = (covidvaccination.loc[i])  
        print(covidvaccination.loc[i])
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