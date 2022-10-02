import mysql.connector
from mysql.connector import Error
import random
import time
import datetime


# Global methods to push interact with the Database

# This method establishes the connection with the MySQL
def create_server_connection(host_name, user_name, user_password):
    # Implement the logic to create the server connection
    conn = None
    try:
        conn = mysql.connector.connect(
            host = host_name,
            user = user_name,
            password = user_password
        )
        print('server creation successful')
    except Error as err:
        print(f"Error: '{err}'")
    return conn
    pass


# This method will create the database and make it an active database
def create_and_switch_database(connection, db_name, switch_db):
    # For database creation use this method
    # If you have created your databse using UI, no need to implement anything
    cursor = connection.cursor()
    try:
        drop_query = "DROP DATABASE IF EXISTS " + db_name
        create_query = "CREATE DATABASE " + db_name
        print("Database created successfully")
        switch_query = "USE " + switch_db
        print("Using Database " + switch_db)
        cursor.execute(drop_query)
        cursor.execute(create_query)
        cursor.execute(switch_query)
        
    except Error as err:
        print(f"Erorr in creation : '{err}'")
    pass

# This method will establish the connection with the newly created DB
def create_db_connection(host_name, user_name, user_password, db_name):
    conn = None
    try:
        conn = mysql.connector.connect(
            host = host_name,
            user = user_name,
            password = user_password,
            database = db_name
        )
        print('Database creation successful')
    except Error as err:
        print(f"Error in creating connection : '{err}'")
    return conn
    pass


# Use this function to create the tables in a database
def create_table(connection, table_creation_statement):
    mycursor = connection.cursor()
    try:
        mycursor.execute(table_creation_statement)
        connection.commit()
        print("Table Creation Successful")
    except Error as err:
        print(f"Error in insert query : '{err}'") 
    pass
 
    


# Perform all single insert statments in the specific table through a single function call
def create_insert_query(connection, query):
    # This method will perform creation of the table
    # this can also be used to perform single data point insertion in the desired table
    mycursor = connection.cursor()
    try:
        mycursor.execute(query)
        connection.commit()
        print("Single Insertion Successful")
    except Error as err:
        print(f"Error in insert query : '{err}'") 
    pass


# retrieving the data from the table based on the given query
def select_query(connection, query):
    # fetching the data points from the table 
    mycursor = connection.cursor()
    result = None
    try:
        mycursor.execute(query)
        result = mycursor.fetchall()
        connection.commit()
        return result
    except Error as err:
        print(f"Error in select query: '{err}'")
    pass


# Execute multiple insert statements in a table
def insert_many_records(connection, sql, val):
    mycursor = connection.cursor()
    try:
        mycursor.executemany(sql, val)
        connection.commit()
        print("Multiple Insertion Successful")
    except Error as err:
        print(f"Error in insert_many query : '{err}'")
    pass
