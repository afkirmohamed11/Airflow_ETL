# Ariflow_ETL


This Airflow ETL pipeline is designed  to efficiently extract data from a MySQL staging area and seamlessly load it into a production PostgreSQL data warehouse. Configured to run every 2 minutes, the pipeline diligently monitors the MySQL staging database for any fresh rows. Upon detection, it performs a slight transformation before appending the data to the PostgreSQL data warehouse. Specifically, a timestamp column is incorporated for each row destined for the final data warehouse, enhancing data traceability and temporal context within the warehouse.

# Project Overview
<img width="802" alt="projectOverview" src="https://github.com/afkirmohamed11/Airflow_ETL/assets/157531189/f49e14f0-f929-4978-88c1-2111fd482350">


# Project Details
1- Run the sales.sql script in MySQL Workbench to create and populate the necessary table in the MySQL staging database.

2- Execute the postgresqlconnect.py script to create and initialize the Data Warehouse schema and table in PostgreSQL.

3- Finally, compose up the yaml file so that you can run the DAG (Directed Acyclic Graph) script to automate the ETL job using Airflow. This DAG script will contain the tasks for extracting data from the MySQL staging database, transforming it if necessary, and loading it into the PostgreSQL Data Warehouse. The DAG will be scheduled to run at the desired intervals every 2 minutes.

## You will need to:
#### create a log directory in the project folder for the dag logs
