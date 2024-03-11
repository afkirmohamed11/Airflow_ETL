import psycopg2
from datetime import datetime


# connectction details

dsn_hostname = '127.0.0.1'
dsn_user='postgres'       
dsn_pwd ='your_password'      
dsn_port ="5432"                
dsn_database ="postgres"           


# create connection

conn = psycopg2.connect(
   database=dsn_database, 
   user=dsn_user,
   password=dsn_pwd,
   host=dsn_hostname, 
   port= dsn_port
)

#Crreate a cursor onject using cursor() method

cursor = conn.cursor()

# create DW nad table
SQL1 = """CREATE schema ProductionDW;
"""
SQL2 = """CREATE TABLE IF NOT EXISTS ProductionDW.sales_data (
  rowid SERIAL PRIMARY KEY,
  product_id INT NOT NULL,
  customer_id INT NOT NULL,
  quantity INT NOT NULL,
  timestamp TIMESTAMP WITHOUT TIME ZONE
);
"""


# Execute the SQL's statement
cursor.execute(SQL1)

cursor.execute(SQL2)

print("Table created")
# Insert data into the sales_data table
insert_sql = """INSERT INTO sales_data (product_id, customer_id, quantity, timestamp) VALUES (%s, %s, %s, %s)"""
timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
data = (8972, 58516, 5, timestamp)

# Execute the SQL statement with data
cursor.execute(insert_sql, data)


conn.commit()
conn.close()



