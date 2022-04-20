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
covidcommunitylevels = pd.read_csv(r'C:\Users\Dane\Desktop\WriteToDatabase\CovidCommunityLevels.csv')


#matching data with inserts
covidcommunitylevels = covidcommunitylevels.astype({"county": str})
covidcommunitylevels = covidcommunitylevels.astype({"county_fips": str})
covidcommunitylevels = covidcommunitylevels.astype({"state": str})
covidcommunitylevels = covidcommunitylevels.astype({"county_population": float})
covidcommunitylevels = covidcommunitylevels.astype({"covid_cases_per_100k": float})
covidcommunitylevels = covidcommunitylevels.astype({"covid-19_community_level": str})
covidcommunitylevels = covidcommunitylevels.astype({"date_updated": str})

#creating raw dates
covidcommunitylevels['date_raw'] = ""

for i in range(0, len(covidcommunitylevels)):
    covidcommunitylevels['date_raw'].loc[i] = covidcommunitylevels['date_updated'].loc[i].replace("-","")
    
covidcommunitylevels['date_key'] = covidcommunitylevels['date_raw'] + covidcommunitylevels['county_fips'] 
covidcommunitylevels = covidcommunitylevels.astype({"date_key": str})
covidcommunitylevels = covidcommunitylevels.astype({"date_raw": str})


#dropping columns not needed for analysis
covidcommunitylevels = covidcommunitylevels.drop(columns=['health_service_area_number', 
                                   'health_service_area',
                                   'health_service_area_population',
                                   'covid_inpatient_bed_utilization',
                                   'covid_hospital_admissions_per_100k',
                                   'date_updated',
                                   'covid_cases_per_100k'
                                   ])


try:
    connection = psycopg2.connect(user="postgres",
                                  password="1988",
                                  host="localhost",
                                  port="5432",
                                  database="CDC_Data")
    cursor = connection.cursor()


    postgres_insert_query = """ INSERT INTO Community_Levels 
    (county,
     county_fips, 
     state, 
     county_population, 
     covid_19_community_level,
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