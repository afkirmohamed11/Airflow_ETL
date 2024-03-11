# Ariflow_ETL


This Airflow ETL pipeline is designed  to efficiently extract data from a MySQL staging area and seamlessly load it into a production PostgreSQL data warehouse. Configured to run every 2 minutes, the pipeline diligently monitors the MySQL staging database for any fresh rows. Upon detection, it performs a slight transformation before appending the data to the PostgreSQL data warehouse. Specifically, a timestamp column is incorporated for each row destined for the final data warehouse, enhancing data traceability and temporal context within the warehouse.


## You will need to:
### create a log directory in the project folder for the dag logs
