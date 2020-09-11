# -*- coding: utf-8 -*-
"""
Created on Wed Sep  2 17:40:58 2020

@author: Manish Chauhan | 9774083186

"""
from tableauhyperapi import HyperProcess, Inserter, Telemetry, Connection, CreateMode, NOT_NULLABLE, NULLABLE, SqlType,TableDefinition,escape_name, escape_string_literal, HyperException, TableName



extract_table = TableDefinition(
   
    table_name=TableName('extract','extract'),
    columns=[
        TableDefinition.Column("Sno", SqlType.text(), NOT_NULLABLE),
        TableDefinition.Column("Name", SqlType.text(), NOT_NULLABLE),
        TableDefinition.Column("Sale", SqlType.big_int()),
        TableDefinition.Column("Profit", SqlType.big_int()),
        TableDefinition.Column("Date", SqlType.text())
    ]
) 
 
data_to_insert = [
        ["1","Shubhi_S",99999,80,""]
    ]        
def run_create_hyper_file_from_csv():

    print("Inside Fucntion to pick data from CSV into table in new Hyper file")
    with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        
        # Replaces file with CreateMode.CREATE_AND_REPLACE if it already exists.
        with Connection(endpoint=hyper.endpoint,
                        database='C:/Users/admin/Desktop/extrp.hyper',
                        create_mode=CreateMode.CREATE_AND_REPLACE) as connection:
             connection.catalog.create_schema('extract')
             connection.catalog.create_table(table_definition=extract_table)
              # create a path that locates CSV file to be used
             path_to_csv = 'C:/Users/admin/Desktop/testing.csv'
     
            # `execute_command` executes a SQL statement and returns the impacted row count.
             count_in_table = connection.execute_command(
                command=f"COPY {extract_table.table_name} from {escape_string_literal(path_to_csv)} with "
                f"(format csv, NULL 'NULL', delimiter ',', header)")
             print(count_in_table)
             # Here we are inserting entries into the extract created above
             with Inserter(connection, TableName('extract','extract')) as inserter:
                     inserter.add_rows(rows=data_to_insert)
                     inserter.execute()
    
#------ This section is for deleting the data from the extract created above.----------------------------

def run_delete_data_in_hyper_file():
    
# Path for hyperfile to be used 
    path_to_etl = 'C:/Users/admin/Desktop/extrp.hyper'


    # Starts the Hyper Process with telemetry enabled to send data to Tableau.
    with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:

        # Connect to existing Hyper file "superstore_sample_delete.hyper".
        with Connection(endpoint=hyper.endpoint, database=path_to_etl) as connection:   
            print("Delete all rows from table with Name=Gandhi")
            ## Here extract.extract is the table name along with schema which was used when we created the hyper file using the extract.See in table defination above
            row_count = connection.execute_command(
                command=f"DELETE FROM extract.extract "
                f"WHERE {escape_name('Name')} = {escape_string_literal('Gandhi')}")

            print(f"The number of deleted rows in table is {row_count}.")

        print("The connection to the Hyper file has been closed.")
    print("The Hyper process has been shut down.")
    
 # Calling functions   
run_create_hyper_file_from_csv()    
run_delete_data_in_hyper_file()
