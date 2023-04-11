### Notes
* Since hourly data calculation is requested, pipeline is scheduled to execute every hour and 05th minute
* During extraction process will capture any missing hours and start loading from that point onwards
* Analytics script is scheduled to run at 05th minute of every hour (in UTC time zone)
* Field of the target table '**devices_summary**' (MySQL table in analytics database) are defined as follows,

| Column name       |Data type| Description                                                              |
|-------------------|---------|--------------------------------------------------------------------------|
| summary_id        |INT      | Incremental ID for the summary table records                                                                         |
| device_id         |VARCHAR  | The unique ID of the device sending the data                             |
| hour_start_time   |DATETIME | Starting time of the aggregated hour                                     |
| max_temperature   |INT      | The maximum temperatures measured for every device per hours             |
| device_data_count |INT      | The amount of data points aggregated for every device per hours          |
| total_distance    |DOUBLE   | Total distance (in KMs) of device movement for every device per hours    |
| inserted_time     |DATETIME | Record inserted time                                                     |


### References
* https://schedule.readthedocs.io/en/stable/examples.html
* https://geopy.readthedocs.io/en/stable/#module-geopy.distance