# -*- coding: utf-8 -*-
"""
Created on Mon Apr 18 05:27:18 2022

@author: Dane
"""

import pandas as pd
import psycopg2

#Reads in the file, drops data not needed before the pandemic and then removes unneeded columns
dimDates = pd.read_csv(r'C:\Users\Dane\Desktop\WriteToDatabase\DimDate.csv')
dimDates = dimDates[dimDates['year'] > 2018]
dimDates = dimDates.drop(columns=['holiday','timezone_id','timezone','timezone_offset'])
dimDates = dimDates.reset_index(drop=True)

#Creates an additional date raw field that removes all of the dashes infront of dates
dimDates['date_raw'] = ""
for i in range(0, len(dimDates)):
    dimDates['date_raw'].loc[i] = dimDates['date'].loc[i].replace("/","")
dimDates


#Does a conversion of all of the data
dimDates = dimDates.astype({"sasdate": str})
dimDates = dimDates.astype({"date_key": str})
dimDates = dimDates.astype({"word_date": str})
dimDates = dimDates.astype({"date": str})
dimDates = dimDates.astype({"year": str})
dimDates = dimDates.astype({"quarter": str})
dimDates = dimDates.astype({"month": str})
dimDates = dimDates.astype({"day_of_month": str})
dimDates = dimDates.astype({"week": str})
dimDates = dimDates.astype({"day_of_week": str})
dimDates = dimDates.astype({"weekday": str})
dimDates = dimDates.astype({"month_and_year": str})
dimDates = dimDates.astype({"date_raw": str})


#Connects to the database
try:
    connection = psycopg2.connect(user="postgres",
                                  password="1988",
                                  host="localhost",
                                  port="5432",
                                  database="CDC_Data")
    cursor = connection.cursor()

    #Queries the table
    postgres_insert_query = """ INSERT INTO DateMapper  
    (sasdate, 
     date_key,
     word_date,
     date,
     Year, 
     Quarter, 
     Month, 
     Day_Of_Month, 
     Week,
     Day_Of_Week,
     Weekday,
     Month_and_year,
     date_raw
     ) 
    VALUES (%s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s)"""

    
    #Iterates through the dataframe to insert the data
    for i in range(0, len(dimDates)):
        record_to_insert = (dimDates.loc[i])
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