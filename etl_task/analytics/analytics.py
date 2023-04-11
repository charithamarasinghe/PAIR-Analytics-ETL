import datetime
import json
import time
from os import environ
from time import sleep
import schedule
import pandas as pd
from geopy import distance
from sqlalchemy import create_engine, MetaData, Table, String, Integer, Column, Float, DateTime, text
from sqlalchemy.exc import OperationalError, SQLAlchemyError
import logging


logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s', datefmt="%Y-%m-%d %H:%M:%S")

logging.info('Waiting for the data generator...')
sleep(20)
logging.info('ETL Starting...')

while True:
    try:
        psql_engine = create_engine(environ["POSTGRESQL_CS"], pool_pre_ping=True, pool_size=10)
        break
    except OperationalError:
        sleep(0.1)
logging.info('Connection to PostgresSQL successful.')

while True:
    try:
        mysql_engine = create_engine(environ["MYSQL_CS"], pool_pre_ping=True, pool_size=10)
        metadata_obj = MetaData()

        devices_summary = Table(
            'devices_summary', metadata_obj,
            Column('summary_id', Integer, primary_key=True , autoincrement=True),
            Column('device_id', String(255)),
            Column('hour_start_time', DateTime),
            Column('max_temperature', Integer),
            Column('device_data_count', Integer),
            Column('total_distance', Float(precision=32)),
            Column('inserted_time', DateTime)
        )
        metadata_obj.create_all(mysql_engine)
        break
    except OperationalError:
        sleep(0.1)
logging.info('Connection to MySQL successful.')


# Write the solution here
def get_last_inserted_time():
    """
    Extract most recent record's time of the target table. New data from this point onwards will be inserted.

    :return: last record inserted timestamp of the target table
    """

    query = """SELECT
                    MAX(hour_start_time) 
                FROM 
                    devices_summary;"""
    conn = mysql_engine.connect()
    try:
        r_set = conn.execute(text(query))
        last_time = r_set.fetchone()
        conn.close()
        return last_time

    except SQLAlchemyError as e:
        logging.error(e)
        conn.close()


def get_first_inserted_time():
    """
    Extract first record's time of the source table.

    :return: first record timestamp of the source table
    """

    query = """SELECT
                    MIN(cast(to_timestamp(time::INTEGER) as timestamp)) as time
                FROM 
                    devices;"""
    conn = psql_engine.connect()
    try:
        r_set = conn.execute(text(query))
        first_time = r_set.fetchone()
        logging.warning(f"No data available in target table, uploading data from: {str(first_time[0])}")
        conn.close()
        return str(first_time[0].replace(hour=00, minute=0, second=0))

    except SQLAlchemyError as e:
        logging.error(e)
        conn.close()


def get_max_temp(start_time, end_time):
    """
    Extract maximum temperature recorded for each device for the given time range.

    :param: start_time: Start time of the considering period (hour)
    :param: end_time: End time of the considering period (hour)

    :return: Maximum temperature recorded for each device for the given range
    """
    query = """SELECT 
                    device_id, 
                    MAX(temperature) as max_temp
                FROM 
                    devices 
                WHERE 
                    cast(to_timestamp(time::INTEGER) as timestamp) between :x AND  :y
                GROUP BY 
                    device_id ;"""
    conn = psql_engine.connect()
    try:
        r_set = conn.execute(text(query), {"x": start_time, "y": end_time})
        data = r_set.fetchall()
        logging.info(f"{str(len(data))} temperature records extracted...")
        conn.close()
        return pd.DataFrame(data, columns=["device_id", "max_temperature"])

    except SQLAlchemyError as e:
        logging.error(e)
        conn.close()


def get_device_data_count(start_time, end_time):
    """
    Extract record count captured for each device for the given time range.

    :param: start_time: Start time of the considering period (hour)
    :param: end_time: End time of the considering period (hour)

    :return: Record count recorded for each device for the given range
    """
    query = """SELECT 
                    device_id, 
                    COUNT(time) as data_count 
                FROM 
                    devices 
                WHERE 
                    cast(to_timestamp(time::INTEGER) as timestamp) between :x AND  :y
                GROUP BY 
                    device_id;"""
    conn = psql_engine.connect()
    try:
        r_set = conn.execute(text(query), {"x": start_time, "y": end_time})
        data = r_set.fetchall()
        logging.info(f"{str(len(data))} data count records extracted...")
        conn.close()
        return pd.DataFrame(data, columns=["device_id", "device_data_count"])

    except SQLAlchemyError as e:
        logging.error(e)
        conn.close()


def flatten_location_data(row):
    """
    Supporting function to flatten location data stored as jsons

    :param: row: Data table's record containing location information

    :return: row: Row with information extracted from JSON as new columns in the row
    """
    location_data = json.loads(row['location'])
    row["latitude"] = location_data["latitude"]
    row["longitude"] = location_data["longitude"]
    return row


def calculate_distance(start_time, end_time):
    """
    Extract location information for each device for the given time range and calculate total distance the device
    travelled.

    :param: start_time: Start time of the considering period (hour)
    :param: end_time: End time of the considering period (hour)

    :return: Total distance of device movement for every device for the given range
    """
    query = """SELECT 
                    device_id, 
                    location
                FROM 
                    devices
                WHERE 
                    cast(to_timestamp(time::INTEGER) as timestamp) between :x AND :y;"""
    conn = psql_engine.connect()
    try:
        r_set = conn.execute(text(query), {"x": start_time, "y": end_time})
        data = r_set.fetchall()
        logging.info(f"{str(len(data))} location records extracted...")
        data = pd.DataFrame(data, columns=["device_id", "location"])
        conn.close()
        if not data.empty:
            device_ids = data['device_id'].unique()
            device_distances = []
            for device_id in device_ids:
                device_data = data[data['device_id'] == device_id]
                device_data = device_data.apply(flatten_location_data, axis=1)

                lats = device_data['latitude'].to_list()
                longs = device_data['longitude'].to_list()

                distances = []
                for n in range(len(device_data) - 1):
                    distances.append(distance.distance(
                        (lats[n], longs[n]),
                        (lats[n + 1], longs[n + 1])).km)

                total_distance = sum(distances)
                device_distances.append({"device_id": device_id, "total_distance": total_distance})

            return pd.DataFrame(device_distances)
    except SQLAlchemyError as e:
        logging.error(e)
        conn.close()


def transform_data(start_time, end_time):
    """
    Calculate all the device summaries using defined logic and merge in to one data table.

    :param: start_time: Start time of the considering period (hour)
    :param: end_time: End time of the considering period (hour)

    :return: summary_df: Data table with summary information
    """
    max_temp_df = get_max_temp(start_time, end_time)
    device_data_count_df = get_device_data_count(start_time, end_time)
    distance_df = calculate_distance(start_time, end_time)

    try:
        if not max_temp_df.empty and not device_data_count_df.empty and not distance_df.empty:
            summary_df = max_temp_df.merge(device_data_count_df, left_on='device_id', right_on='device_id')
            summary_df = summary_df.merge(distance_df, left_on='device_id', right_on='device_id')
            return summary_df
        else:
            return pd.DataFrame()
    except Exception as error:
        logging.error(error)
        return pd.DataFrame()


def format_data(summary):
    """
    Format summary table to be uploaded while inserting updating time.

    :param: summary: Summary table created for the given time range

    :return: summary: Formatted summary table created for the given time range
    """
    columns = ['device_id', 'hour_start_time', 'max_temperature', 'device_data_count', 'total_distance']
    summary = summary[columns]
    summary = summary.assign(inserted_time=str(datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')))
    return summary


def load_data(summary):
    """
    Load summary data to MySQL database

    :param: summary: Summary table created for the given time range

    :return: None
    """
    logging.info("Inserting data to mysql...")
    conn = mysql_engine.connect()
    try:
        query = """INSERT INTO devices_summary 
                        (device_id ,
                        hour_start_time ,
                        max_temperature ,
                        device_data_count, 
                        total_distance,
                        inserted_time)
                     VALUES
                        (:device_id, 
                        :hour_start_time, 
                        :max_temperature, 
                        :device_data_count, 
                        :total_distance,
                        :inserted_time);"""

        logging.info(f"{str(len(summary))} records ready to be uploaded")
        result = conn.execute(text(query), summary.to_dict("records"))
        conn.commit()
        logging.info(f"{str(result.rowcount)} records uploaded to the target table")
        conn.close()
        time.sleep(2)

    except SQLAlchemyError as e:
        logging.error(e)
        conn.close()


def resolve_last_inserted_time():
    last_inserted_time = get_last_inserted_time()[0]
    if last_inserted_time is not None:
        logging.info(f"Last record inserted hour: {str(last_inserted_time)}")
        return str(last_inserted_time + datetime.timedelta(hours=1))
    else:
        return get_first_inserted_time()


def run_etl():
    """
    Runs ETL task. Initiate and control Extraction, transformation and loading tasks

    :param: None

    :return: None
    """
    logging.info(f"Starting data extraction at {str(datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'))} UTC...")

    inserted_time = resolve_last_inserted_time()
    current_time = datetime.datetime.utcnow()

    series_to_be_extracted = pd.Series(pd.date_range(
        start=inserted_time,
        end=f'{str(current_time.date())} {str(datetime.datetime.utcnow().hour)}:00:00', freq='H'))
    logging.info(f"series_to_be_extracted  {', '.join(list(map(lambda x: str(x), series_to_be_extracted.to_list())))}")

    for sr_item in series_to_be_extracted.to_list():
        logging.info(f"Processing for the hour starting at: {str(sr_item)}")
        end_time = sr_item.replace(minute=59, second=59)

        summary = transform_data(start_time=str(sr_item), end_time=str(end_time))
        if not summary.empty:
            logging.info(f"{len(summary)} summary records generated...")
            summary = summary.assign(hour_start_time=str(sr_item))

            summary = format_data(summary=summary)
            load_data(summary=summary)


# Run job every hour at the 5th minute (In UTC time zone)
schedule.every().hour.at(":05", "UTC").do(run_etl)

while True:
    schedule.run_pending()
    time.sleep(1)

