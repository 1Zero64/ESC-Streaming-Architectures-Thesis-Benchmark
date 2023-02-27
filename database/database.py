# python3 database.py
# -*- coding: utf-8 -*-
# ===========================================================================================
# @author 1Zero64
# Description: Connection to PostgreSQL database and functions to retrieve the datasets as data frames
# ===========================================================================================

import psycopg2 as pg
import pandas as pd
from dotenv import load_dotenv
import os


# Method to connect to the Postgres database with environment variables
def connect():
    # Load .env variables
    load_dotenv()

    # Connect to the database
    return pg.connect(
        host=os.environ['DB_HOST'],
        database=os.environ['DB_NAME'],
        port=os.environ['DB_PORT'],
        user=os.environ['DB_USER'],
        password=os.environ['DB_PASSWORD']
    )


# Retrieve event store data, prepare and return them as a data frame
def data_frame_from_event_store():
    # Get a cursor from a pg connection
    connection = connect()
    cursor = connection.cursor()

    # Execute the query and prepare the column names
    event_store_query = "SELECT * FROM event_store order by id"
    event_store_column_names = ['id', 'created_on', 'event_stream', 'humidity', 'processed_on', 'sensor_id', 'temperature']

    # Execute query with cursor and save all results in a tuples list
    cursor.execute(event_store_query)
    tuples_list = cursor.fetchall()

    # Close connection
    connection.close()
    cursor.close()

    # Create a data frame from the data and columns
    data_frame = pd.DataFrame(tuples_list, columns=event_store_column_names)

    # Return the data frame
    return data_frame


# Retrieve materialized view data, prepare and return them as a data frame
def data_frame_from_materialized_view():
    # Get a cursor from a pg connection
    connection = connect()
    cursor = connection.cursor()

    # Execute the query and prepare the column names
    materialized_view_query = "SELECT * FROM materialized_view ORDER BY id"
    materialized_view_column_names = ['id', 'created_on', 'danger', 'event_stream', 'humidity', 'latency', 'processed_on', 'sensor_id', 'temperature']

    # Execute query with cursor and save all results in a tuples list
    cursor.execute(materialized_view_query)
    tuples_list = cursor.fetchall()

    # Close connection
    connection.close()
    cursor.close()

    # Create a data frame from the data and columns
    data_frame = pd.DataFrame(tuples_list, columns=materialized_view_column_names)

    # Return the data frame
    return data_frame
