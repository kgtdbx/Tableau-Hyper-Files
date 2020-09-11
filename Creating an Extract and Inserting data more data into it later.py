# -*- coding: utf-8 -*-
"""
Created on Thu Jul  2 21:35:47 2020

@author: Manish Chauhan || +91-9774083186
"""
from tableauhyperapi import HyperProcess, Inserter, Telemetry, Connection, CreateMode, NOT_NULLABLE, NULLABLE, SqlType,TableDefinition,escape_name, escape_string_literal, HyperException, TableName

# Starting local hyper instance


extract_table = TableDefinition(
   
    table_name=TableName('extract','extract'),
    columns=[
        TableDefinition.Column("Sno", SqlType.text(), NOT_NULLABLE),
        TableDefinition.Column("Name", SqlType.text(), NOT_NULLABLE),
        TableDefinition.Column("Sale", SqlType.big_int()),
        TableDefinition.Column("Profit", SqlType.big_int()),
        #TableDefinition.Column("Date", SqlType.date())
    ]
) 
 
data_to_insert = [
        ["1","Vishakha",99999,80]
    ]        
def run_create_hyper_file_from_csv():

    print("Inside Fucntion to pick data from CSV into table in new Hyper file")
    with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        
        # Replaces file with CreateMode.CREATE_AND_REPLACE if it already exists
        with Connection(endpoint=hyper.endpoint,
                        database='C:/Users/admin/Desktop/extrp1.hyper',
                        create_mode=CreateMode.CREATE_AND_REPLACE) as connection:
             connection.catalog.create_schema('extract')
             connection.catalog.create_table(table_definition=extract_table)
              # create a path that locates CSV file to be used
             path_to_csv = 'C:/Users/admin/Desktop/testing1.csv'

             
            # `execute_command` executes a SQL statement and returns the impacted row count.
             count_in_table = connection.execute_command(
                command=f"COPY {extract_table.table_name} from {escape_string_literal(path_to_csv)} with "
                f"(format csv, NULL 'NULL', delimiter ',', header)")
             print(count_in_table)
             with Inserter(connection, TableName('extract','extract')) as inserter:
                     inserter.add_rows(rows=data_to_insert)
                     inserter.execute()


#Creating .hyper file by calling the function
run_create_hyper_file_from_csv()




    

