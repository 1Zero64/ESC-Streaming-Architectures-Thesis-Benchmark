import psycopg2 as pg
import pandas as pd
from dotenv import load_dotenv
import os


def connect():
    load_dotenv()

    return pg.connect(
        host=os.environ['DB_HOST'],
        database=os.environ['DB_DATABASE'],
        port=os.environ['DB_PORT'],
        user=os.environ['DB_USERNAME'],
        password=os.environ['DB_PASSWORD']
    )


def data_frame_from_event_store():
    connection = connect()
    cursor = connection.cursor()

    event_store_query = "SELECT * FROM event_store order by id"
    event_store_column_names = ['id', 'sensorId', 'temperature', 'humidity', 'event_stream', 'created_on',
                                'processed_on']

    cursor.execute(event_store_query)
    tuples_list = cursor.fetchall()

    connection.close()
    cursor.close()

    data_frame = pd.DataFrame(tuples_list, columns=event_store_column_names)

    return data_frame


def data_frame_from_materialized_view():
    connection = connect()
    cursor = connection.cursor()

    materialized_view_query = "SELECT * FROM materialized_view ORDER BY id"
    materialized_view_column_names = ['id', 'sensorId', 'temperature', 'humidity', 'event_stream', 'danger',
                                      'created_on', 'processed_on', 'latency']

    cursor.execute(materialized_view_query)
    tuples_list = cursor.fetchall()

    connection.close()
    cursor.close()

    data_frame = pd.DataFrame(tuples_list, columns=materialized_view_column_names)

    return data_frame

if __name__ == '__main__':
    data_frame_from_materialized_view()
    print(data_frame_from_materialized_view().head())