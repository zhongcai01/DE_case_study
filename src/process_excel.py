import pandas as pd
import psycopg2
import io
import json


with open('constants.json', 'r') as f:
        constants = json.load(f)

extracted_folder_path = constants["extracted_folder_path"]

postgresql_host = constants["postgresql_host"]

postgresql_port = constants["postgresql_port"]

postgresql_database = constants["postgresql_database"]

postgresql_user = constants["postgresql_user"]

postgresql_password = constants["postgresql_password"]



def process_excel(extracted_folder_path):

    # load all csv
    service = pd.read_csv(f"{extracted_folder_path}/Raw Service.csv")
    customer = pd.read_csv(f"{extracted_folder_path}/Raw Customer.csv")
    order = pd.read_csv(f"{extracted_folder_path}/Raw Orders.csv")
    active = pd.read_csv(f"{extracted_folder_path}/Raw Active.csv")

    # Merge servuce and order csv on "SERVICE_ID" and REPORT_DATE
    ServiceOrder = pd.merge(service, order, how='left', on=["SERVICE_ID","REPORT_DATE"])

    # Merge customer and active csv
    # Need to merge customer csv to active csv 
    # as service and order does not have "CUSTOMER_ID"

    CustomerActive = pd.merge(customer, active, how='left', on=["CUSTOMER_ID"])

    # Drop NA columns
    ServiceOrder.dropna(subset="ORDER_TYPE", inplace=True)
    CustomerActive.dropna(subset="SERVICE_ID", inplace=True)

    # rename columns to prepare for another merge
    CustomerActive.rename(columns={
        "REPORT_DATE_y":"ACTIVE_SUB_DATE",
        "REPORT_DATE_x":"REPORT_DATE"
    }, inplace=True)

    # Merge ServiceOrder and CustomerActive dataframe
    ServiceOrderCustomerActive = pd.merge(ServiceOrder, CustomerActive, how='left', on=["SERVICE_ID","REPORT_DATE"])

    # Drop NA in "CUSTOMER_ID" column
    ServiceOrderCustomerActive.dropna(subset="CUSTOMER_ID", inplace=True)

    ServiceOrderCustomerActive.drop(columns="SERVICE_NAME_y", inplace=True)
    ServiceOrderCustomerActive.rename(columns={"SERVICE_NAME_x":"SERVICE_NAME"},inplace=True)

    # Change float to int for CUSTOMER_ID
    ServiceOrderCustomerActive["CUSTOMER_ID"] = ServiceOrderCustomerActive["CUSTOMER_ID"].astype(int)

    # Change date columns to datetime type
    ServiceOrderCustomerActive["REPORT_DATE"] = pd.to_datetime(ServiceOrderCustomerActive["REPORT_DATE"], format='%Y-%m-%d', errors='raise')
    ServiceOrderCustomerActive["ACTIVE_SUB_DATE"] = pd.to_datetime(ServiceOrderCustomerActive["ACTIVE_SUB_DATE"], format='%Y-%m-%d', errors='raise')

    # OrderTable
    # Grab only necessary columns for OrderTable
    columns_to_copy = ["REPORT_DATE","SERVICE_ID","SERVICE_NAME","SERVICE","ORDER_TYPE","ORDER_TYPE_L2","CUSTOMER_ID","CUSTOMER_SEGMENT_FLAG","CUSTOMER_GENDER","CUSTOMER_NATIONALITY"]
    OrderTable = ServiceOrderCustomerActive[columns_to_copy].copy()


    # Activetable
    ActiveTable = pd.merge(service, active, how='inner', on="SERVICE_ID")
    ActiveTable.rename(columns={
        "SERVICE_NAME_x":"SERVICE_NAME",
        "REPORT_DATE_x":"REPORT_DATE",
        "REPORT_DATE_y":"ACTIVE_SUB_DATE"
    }, inplace=True)
    ActiveTable.drop(columns="SERVICE_NAME_y", inplace=True)

    return OrderTable, ActiveTable

def connect_to_database():
    connection = psycopg2.connect(
        host=postgresql_host,
        port=postgresql_port,
        database=postgresql_database,
        user=postgresql_user,
        password=postgresql_password
    )
    return connection

def create_table_in_database(connection):
    try:
        cursor = connection.cursor()
        create_ordertable_query = """
        CREATE TABLE IF NO EXISTS OrderTable (
            REPORT_DATE	DATE
            SERVICE_ID INT
            SERVICE_NAME VARCHAR(255)	
            SERVICE	VARCHAR(255)
            ORDER_TYPE VARCHAR(255)
            ORDER_TYPE_L2 VARCHAR(255)
            CUSTOMER_ID	INT
            CUSTOMER_SEGMENT_FLAG VARCHAR(50)
            CUSTOMER_GENDER	VARCHAR(1)
            CUSTOMER_NATIONALITY VARCHAR(50)
        );
        """
        cursor.execute(create_ordertable_query)
        connection.commit()
        

        create_activetable_query = """
        CREATE TABLE IF NO EXISTS ActiveTable (
            REPORT_DATE	DATE
            SERVICE_ID VARCHAR(255)
            SERVICE_NAME VARCHAR(255)
            SERVICE VARCHAR(255)
            ACTIVE_SUB_DATE	DATE
            CUSTOMER_ID	INT
            SUBSCRIPTION_STATUS VARCHAR(50)
        );
        """
        cursor.execute(create_activetable_query)
        connection.commit()

    except Exception as e:
        connection.rollback()
    finally:
        if cursor:
            cursor.close()

def insert_ordertable_to_database(connection, dataframe):
    try:
        cursor = connection.cursor()
        buffer = io.StringIO()

        dataframe.to_csv(buffer, index=False, header=False)

        cursor.copy_from(buffer, 'OrderTable', sep=',', 
                         columns=("REPORT_DATE","SERVICE_ID","SERVICE_NAME", 
                                  "SERVICE",	"ORDER_TYPE", "ORDER_TYPE_L2", 
                                  "CUSTOMER_ID", "CUSTOMER_SEGMENT_FLAG", "CUSTOMER_GENDER", 
                                  "CUSTOMER_NATIONALITY"))
        connection.commit()
    except Exception as e:
        connection.rollback()
    finally:
        if cursor:
            cursor.close()

def insert_activetable_to_database(connection, dataframe):
    try:
        cursor = connection.cursor()
        buffer = io.StringIO()

        dataframe.to_csv(buffer, index=False, header=False)

        cursor.copy_from(buffer, 'ActiveTable', sep=',', 
                         columns=(
                            "REPORT_DATE",
                            "DATE",
                            "SERVICE_ID",
                            "SERVICE_NAME",
                            "SERVICE",
                            "ACTIVE_SUB_DATE",
                            "CUSTOMER_ID",
                            "SUBSCRIPTION_STATUS"))
        connection.commit()
    except Exception as e:
        connection.rollback()
    finally:
        if cursor:
            cursor.close()

def process_excel_and_insert_to_database():


    OrderTable, ActiveTable = process_excel(extracted_folder_path)
    conn = connect_to_database()

    create_table_in_database(conn)
    insert_ordertable_to_database(conn, OrderTable)
    insert_activetable_to_database(conn, ActiveTable)

if __name__ == '__main__':
    process_excel_and_insert_to_database()    
