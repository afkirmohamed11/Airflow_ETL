# Ariflow_ETL

This is an Airflow ETL pipeline designed to extract data from a staging area in a MySQL database and load it into a production PostgreSQL data warehouse. The pipeline is set to run at intervals of 2 minutes to check for any new rows added in the MySQL staging database and then append them to the existing data in the PostgreSQL data warehouse.
