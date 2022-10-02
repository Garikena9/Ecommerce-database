import database as db

# Driver code
if __name__ == "__main__":
    """
    Please enter the necessary information related to the DB at this place. 
    Please change PW and ROOT based on the configuration of your own system. 
    """
    PW = "Susanth@1203"  # IMPORTANT! Put your MySQL Terminal password here.
    ROOT = "root"
    DB = "ecommerece_record"  # This is the name of the database we will create in the next step - call it whatever
    # you like.
    LOCALHOST = "localhost"
   # connection = db.create_server_connection(LOCALHOST, ROOT, PW)

    
    # creating the schema in the DB 
    #db.create_and_switch_database(connection, DB, DB)
    
    # Start implementing your task as mentioned in the problem statement 
    # Implement all the test cases and test them by running this file
    #*-----------SOLUTION 2B----------------*
    print("Executing 5 insert operations as per 2B ")
    insert_query = '''
            INSERT INTO ORDERS (ORDER_ID, CUSTOMER_ID, VENDOR_ID, TOTAL_VALUE, ORDER_QUANTITY, REWARD_POINT)
            VALUES (%s, %s, %s, %s, %s, %s)
    '''
    record_to_insert = [(101,'10','3',16800,4,200),
                         (102,'9','2',15990,3,100),
                         (103,'8','1',15500,2,700),
                         (104,'7','9',12600,5,400),
                         (105,'5','6',10700,9,800)]

    connection = db.create_db_connection(LOCALHOST, ROOT, PW, DB)
    db.insert_many_records(connection, insert_query,record_to_insert)
    print("5 records inserted sucessfully")

    #*------------SOLUTION 2C---------------*
    print("Showing Data inserted in Orders Table")
    show_orders_table = """
        SELECT * FROM ORDERS
    """
    query = db.select_query(connection, show_orders_table)
    for r in query:
        print(r)
    print("Displayed records in Orders Table successful")

    #*----------------SOLUTION 3A-------------------*

    min_select_query = '''
        SELECT * FROM ORDERS WHERE TOTAL_VALUE = (SELECT MIN(TOTAL_VALUE) FROM ORDERS);
    '''

    min_order = db.select_query(connection, min_select_query)
    print("Orders with min value is: ")
    print(min_order)

    max_select_query = '''
        SELECT * FROM ORDERS WHERE TOTAL_VALUE = (SELECT MAX(TOTAL_VALUE) FROM ORDERS); 
    '''
    max_order = db.select_query(connection, max_select_query)
    print("Orders with Max value is: ")
    print(max_order)

    #*---------------SOLUTION 3B--------------------*
    print("Printing all orders greater than the average value")

    avg_select_query = '''
        SELECT * FROM ORDERS WHERE TOTAL_VALUE > (SELECT AVG(TOTAL_VALUE) FROM ORDERS);
    '''
    avg_order = db.select_query(connection, avg_select_query)
    print("Orders greater than average value : ")
    for result in avg_order:
        print(result)

    #*------------SOLUTION 3C-----------------------------*
    print("Fetching data as per 3C problem statement")
    fetch_query = '''
        SELECT A.CUSTOMER_ID, MAX(A.TOTAL_VALUE) AS MAX_VALUE, C.USER_NAME, C.USER_EMAIL FROM ECOMMERECE_RECORD.ORDERS A
        LEFT JOIN ECOMMERECE_RECORD.USERS C ON A.CUSTOMER_ID = C.USER_ID
        GROUP BY A.CUSTOMER_ID;
    ''' 
    highest_pruchase_per_customer = db.select_query(connection, fetch_query)
    for result in highest_pruchase_per_customer:
        print(result)
    print("fetch completed")


    