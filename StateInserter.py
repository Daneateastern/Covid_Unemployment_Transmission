# -*- coding: utf-8 -*-
"""
Created on Mon Apr 01 04:30:33 2022

@author: Dane
"""

import pandas as pd
import psycopg2

#reading in data
stateAbbrevs = pd.read_csv(r'C:\Users\Dane\Desktop\WriteToDatabase\StateAbbrevs.csv')

try:
    connection = psycopg2.connect(user="postgres",
                                  password="1988",
                                  host="localhost",
                                  port="5432",
                                  database="CDC_Data")
    cursor = connection.cursor()

    postgres_insert_query = """ INSERT INTO State_Mapper  
    (State, Abbrev, Code) 
    VALUES (%s,%s,%s)"""


    #iterating through dataframe and inserting rows
    for i in range(0, len(stateAbbrevs)):
        record_to_insert = (stateAbbrevs.loc[i])
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