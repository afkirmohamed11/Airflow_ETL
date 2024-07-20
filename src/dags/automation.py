import os
from dotenv import load_dotenv
import mysql.connector
import psycopg2
from datetime import datetime
import logging


# Load environment variables from .env file
load_dotenv()

# Get Mysql  db environment variables

MYSQL_HOST = os.getenv('MYSQL_HOST')
MYSQL_USER = os.getenv('MYSQL_USER')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD')
MYSQL_DATABASE = os.getenv('MYSQL_DATABASE') 

# Get Postgres db environment variables
POSTGRES_HOST = os.getenv('DSN_HOSTNAME')
POSTGRES_USER = os.getenv('DSN_USER')
POSTGRES_PASSWORD = os.getenv('DSN_PWD')
POSTGRES_PORT = os.getenv('DSN_PORT')
POSTGRES_DATABASE = os.getenv('DSN_DATABASE') 

def get_last_rowid():
    """Retrieves the last rowid from the sales_data table in PostgreSQL."""
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            database=POSTGRES_DATABASE,
            port=POSTGRES_PORT
        )
        cursor = conn.cursor()
        cursor.execute("SELECT MAX(rowid) FROM sales_data")
        last_rowid = cursor.fetchone()[0]
        logging.info("Last rowid retrieved: %s", last_rowid)
        return last_rowid
    except (Exception, psycopg2.Error) as error:
        logging.error("Error while getting last rowid: %s", error)
        print("Error while getting last rowid:", error)
        return None
    finally:
        if 'conn' in locals():
            cursor.close()
            conn.close()


def get_latest_records(rowid):
    """Fetches records with rowid greater than the provided id from MySQL's sales_data table."""

    cursor = None  
    
    try:
        # Connect to MySQL
        mydb = mysql.connector.connect(
            user = MYSQL_USER,
            password = MYSQL_PASSWORD,
            host = MYSQL_HOST,
            database = MYSQL_DATABASE,
            auth_plugin = 'mysql_native_password'
        )
        cursor = mydb.cursor()

        cursor.execute("SELECT * FROM sales.sales WHERE rowid > %s", (rowid,))
        new_records = cursor.fetchall()

        return new_records

    except (Exception, mysql.connector.Error) as error: 
        print("Error while getting latest records:", error)
        return []  

    finally:
        if cursor:
            cursor.close()
        if 'mydb' in locals():
            mydb.close()




def insert_records(records):
  """Inserts a list of records into PostgreSQL's sales_data table."""
  try:
    # Connect to PostgreSQL
    conn = psycopg2.connect(
      host=POSTGRES_HOST,
      user=POSTGRES_USER,
      password=POSTGRES_PASSWORD,
      database=POSTGRES_DATABASE,
      port=POSTGRES_PORT
    )

    cursor = conn.cursor()

    # Add timestamp for each record
    for record in records:
        current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        record_with_timestamp = record + (current_timestamp,)  
        SQL = """INSERT INTO sales_data (rowid, product_id, customer_id, quantity, timestamp)
                VALUES (%s, %s, %s, %s, %s)"""
        cursor.execute(SQL, record_with_timestamp)


    

    conn.commit()

  except (Exception, psycopg2.Error) as error:
    print("Error while inserting records:", error)

  finally:
    if 'conn' in locals():
      cursor.close()
      conn.close()



if __name__ == "__main__":
    # Get last rowid from production data warehouse
    last_row_id = get_last_rowid()
    print("Last row id on production datawarehouse =", last_row_id)

    if last_row_id:  # Proceed only if last_row_id is retrievedd successfully
        # Find new records in staging datawarehouse(MySQL DB)
        new_records = get_latest_records(last_row_id)
        print("New rows on staging datawarehouse =", len(new_records))
        insert_records(new_records)
