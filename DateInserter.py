# -*- coding: utf-8 -*-
"""
Created on Mon Apr 18 05:27:18 2022

@author: Dane
"""

import pandas as pd
import psycopg2


dimDates = pd.read_csv(r'C:\Users\Dane\Desktop\WriteToDatabase\DimDate.csv')
dimDates = dimDates[dimDates['year'] > 2018]
dimDates = dimDates.drop(columns=['holiday','timezone_id','timezone','timezone_offset'])
dimDates = dimDates.reset_index(drop=True)

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



try:
    connection = psycopg2.connect(user="postgres",
                                  password="1988",
                                  host="localhost",
                                  port="5432",
                                  database="CDC_Data")
    cursor = connection.cursor()

    postgres_insert_query = """ INSERT INTO DimDate  
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
     Month_and_year
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
            %s)"""
    """
    record_to_insert = (dimDates.loc[0])
    cursor.execute(postgres_insert_query, record_to_insert)
    connection.commit()
    count = cursor.rowcount
    print("Record inserted successfully into mobile table")
    """
    
    
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