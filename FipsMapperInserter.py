# -*- coding: utf-8 -*-
"""
Created on Mon Apr 01 12:44:56 2022

@author: Dane
"""

#Importing pandas and psycop
import pandas as pd
import psycopg2

#Reading in the file
fipsmaster = pd.read_csv(r'C:\Users\Dane\Desktop\WriteToDatabase\state_and_county_fips_master.csv')

#Converting all the data to strings if they are not already
fipsmaster = fipsmaster.astype({"fips": str})
fipsmaster = fipsmaster.astype({"name": str})
fipsmaster = fipsmaster.astype({"state": str})


try:
    #Local connection to the current database called CDC_Data
    connection = psycopg2.connect(user="postgres",
                                  password="1988",
                                  host="localhost",
                                  port="5432",
                                  database="CDC_Data")
    cursor = connection.cursor()

    #Connects straight to the table and match the values with columns

    postgres_insert_query = """ INSERT INTO FIPS_Code_Mapper  
    (fips, county, state) 
    VALUES (%s,%s,%s)"""
    
    #Commented out to test one insert
    
    """
    record_to_insert = (state_unemployment.loc[0])  
    cursor.execute(postgres_insert_query, record_to_insert)
    connection.commit()
    count = cursor.rowcount
    print("Record inserted successfully into mobile table")
    """
    
    #iterates through the dataframe and then inserts each row
    for i in range(0,len(fipsmaster)):
        record_to_insert = (fipsmaster.loc[i])  
        print(fipsmaster.loc[i])
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
