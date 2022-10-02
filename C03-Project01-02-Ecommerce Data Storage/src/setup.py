import csv
import database as db

PW = "Susanth@1203"  # IMPORTANT! Put your MySQL Terminal password here.
ROOT = "root"
DB = "ecommerece_record"  # This is the name of the database we will create in the next step - call it whatever you like.
LOCALHOST = "localhost"  # considering you have installed MySQL server on your computer

RELATIVE_CONFIG_PATH = 'D:/projects/C03-Project01-02-Ecommerce Data Storage/config/'

USERS = 'users'
PRODUCTS = 'products'
ORDERS = 'orders'

connection = db.create_server_connection(LOCALHOST, ROOT, PW)

# creating the schema in the DB
db.create_and_switch_database(connection, DB, DB)

# Create the tables through python code here
#Creating Users Table
create_users_table = '''
    CREATE TABLE USERS (
        USER_ID VARCHAR(10) PRIMARY KEY,
        USER_NAME VARCHAR(45),
        USER_EMAIL VARCHAR(45),
        USER_PASSWORD VARCHAR(45),
        USER_ADDRESS VARCHAR(45),
        IS_VENDOR TINYINT(1) 
    );
'''
#Creating Products Table
create_products_table = '''
    CREATE TABLE PRODUCTS (
        PRODUCT_ID VARCHAR(45) PRIMARY KEY,
        PRODUCT_NAME VARCHAR(45),
        PRODUCT_PRICE DOUBLE,
        PRODUCT_DESCRIPTION VARCHAR(100),
        VENDOR_ID VARCHAR(10),
        EMI_AVAILABLE VARCHAR(10),
        CONSTRAINT FK1_VENDOR_ID FOREIGN KEY (VENDOR_ID) REFERENCES USERS (USER_ID)
    );
'''
#Creating Orders Table
create_orders_table = '''
    CREATE TABLE ORDERS (
        ORDER_ID INT PRIMARY KEY,
        CUSTOMER_ID VARCHAR(10),
        VENDOR_ID VARCHAR(10),
        TOTAL_VALUE DOUBLE,
        ORDER_QUANTITY INT,
        REWARD_POINT INT,
        CONSTRAINT FK2_VENDOR_ID FOREIGN KEY (VENDOR_ID) REFERENCES USERS(USER_ID),
        CONSTRAINT FK1_CUSTOMER_ID FOREIGN KEY (CUSTOMER_ID) REFERENCES USERS (USER_ID)
    );
'''
#Creating Customer LeaderBoard
create_customer_leaderboard_table = '''
    CREATE TABLE CUSTOMER_LEADERBOARD (
        CUSTOMER_ID VARCHAR(10) PRIMARY KEY,
        TOTAL_VALUE DOUBLE,
        CUSTOMER_NAME VARCHAR(50),
        CUSTOMER_EMAIL VARCHAR(50),
        CONSTRAINT FK2_CUSTOMER_ID FOREIGN KEY (CUSTOMER_ID)
        REFERENCES USERS (USER_ID)
    );
    '''
# if you have created the table in UI, then no need to define the table structure
# If you are using python to create the tables, call the relevant query to complete the creation
db.create_table(connection, create_users_table)
print("Users Table Created")
db.create_table(connection, create_products_table)
print("Products Table Created")
db.create_table(connection, create_orders_table)
print("Orders Table Created")
db.create_table(connection, create_customer_leaderboard_table)
print("Customer_Leaderboard Table Created")

with open(RELATIVE_CONFIG_PATH + USERS+ '.csv', 'r') as f:
    val = []
    data = csv.reader(f)
    for row in data:
        val.append(tuple(row))
        # user_insert_query = '''
        #     INSERT INTO USERS (USER_ID, USER_NAME, USER_EMAIL, USER_PASSWORD, USER_ADDRESS, IS_VENDOR)
        #     VALUES (%s, %s, %s, %s, %s, %s)
        # '''
    val.pop(0)
    # db.insert_many_records(connection, user_insert_query, val)
    """
    Here we have accessed the file data and saved into the val data struture, which list of tuples. 
    Now you should call appropriate method to perform the insert operation in the database. 
    """
    #print(val)
    user_insert_query = '''
        INSERT INTO USERS (USER_ID, USER_NAME, USER_EMAIL, USER_PASSWORD, USER_ADDRESS, IS_VENDOR)
        VALUES (%s, %s, %s, %s, %s, %s)
    '''
    db.insert_many_records(connection, user_insert_query, val)

with open(RELATIVE_CONFIG_PATH + PRODUCTS+ '.csv', 'r') as f:
    val = []
    data = csv.reader(f)
    for row in data:
        val.append(tuple(row))
        products_insert_query = '''
            INSERT INTO PRODUCTS (PRODUCT_ID, PRODUCT_NAME, PRODUCT_PRICE, PRODUCT_DESCRIPTION, VENDOR_ID, EMI_AVAILABLE)
            VALUES (%s, %s, %s, %s, %s, %s)
        '''
    val.pop(0)
    db.insert_many_records(connection, products_insert_query, val)
    """
    Here we have accessed the file data and saved into the val data struture, which list of tuples. 
    Now you should call appropriate method to perform the insert operation in the database. 
    """

with open(RELATIVE_CONFIG_PATH + ORDERS + '.csv', 'r') as f:
    val = []
    data = csv.reader(f)
    for row in data:
        val.append(tuple(row))
        order_insert_query = '''
            INSERT INTO ORDERS (ORDER_ID, CUSTOMER_ID, VENDOR_ID, TOTAL_VALUE, ORDER_QUANTITY, REWARD_POINT)
            VALUES (%s, %s, %s, %s, %s, %s)
        '''
    val.pop(0)
   # print(val)
    db.insert_many_records(connection, order_insert_query, val)
    """
    Here we have accessed the file data and saved into the val data struture, which list of tuples. 
    Now you should call appropriate method to perform the insert operation in the database. 
    """
