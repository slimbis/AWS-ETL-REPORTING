## ETL processing pipeline for COVID-19 data using AWS services and Python

This project is for an automatic ETL processin pipeline for Covid-19 data for Greece (or any other selected country) using AWS services and Python code.

The data are downloaded from John Hopkins CSSE's github repository (https://github.com/CSSEGISandData) and specifically the time series data (https://github.com/CSSEGISandData/COVID-19/tree/master/csse_covid_19_data/csse_covid_19_time_series). The data are updated daily around 23:59 (UTC).

The pipeline uses the following:
* an Amazon EventBridge rule (every 24h) to trigger a Amazon Lambda function
* a Amazon Lambda function that load the appropriate libraries, the boto sessions, the sns messaging, the database connection (postgres) and the table creation and update logic
* a Amazon Lambda layers for Pandas, Psycopg2 and data transformaton function.
* a Amazon PostgreSQL RDS database
* a SNS topic for email notfications on the processing pipeline (errors and successful loads)

A Redash connection to the PostgreSQL database and dashboard was created for data presentation and public access to the interested parties.
