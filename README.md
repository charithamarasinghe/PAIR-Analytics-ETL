# PAIR-Analytics-ETL

* etl_task folder contains code without binaries
* etl_task/analytics/analytics.py script contains code for the ETL logic

### Summary
The data generated (by the simulator) is pulled (from PostgresSQL database), transformed and saved into a new database (MySQL database). ETL pipeline does the following:
* Pull the data from PostgresSQL
* During tanformation following data aggregations are calculated:
    * The maximum temperatures measured for every device per hours.
    * The amount of data points aggregated for every device per hours.
    * Total distance of device movement for every device per hours.
* Resulting aggregated data will be stored in the MySQL database
