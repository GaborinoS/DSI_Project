from utils.kafka_connector import run_consumer
from confluent_kafka import Producer, KafkaError

import utils.ccloud_lib as ccloud_lib
import pandas as pd

import influxdb_client
from influxdb import DataFrameClient
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

from dateutil import parser
import requests
from string import Template
import yfinance as yf
import datetime



def process_data(data):
    df = pd.read_json(data).T
    df = df.astype(float)
    df.index.name = 'Datetime'

    print(df.head())

    #InfluxDB connection
    bucket = "DSI_test"
    token ="GMbnHWGhM9p9t9mIjbc1I5KlWir8LJxBDkpMF0SiOa56f1nvLepCEN7iI_5-sR80FA8CvLmf_mHcy8Gc5XYwvA=="
    org="dsi"
    client = InfluxDBClient(url="http://localhost:8086", token=token, org=org)


    with client.write_api(write_options=SYNCHRONOUS) as writer:
        writer.write(
            bucket=bucket,
            record=df,
            data_frame_measurement_name="9",
            data_frame_tag_columns=["symbol"],
            data_frame_field_columns=["Open", "High", "Low", "Close", "Adj Close", "Volume"]
            )

if __name__ == '__main__':


    # Read arguments and configurations and initialize
    args = ccloud_lib.parse_args()
    conf = ccloud_lib.read_ccloud_config(args.config_file)

    # Create Consumer instance
    consumer_conf = ccloud_lib.pop_schema_registry_params_from_config(conf)
    run_consumer(conf, 'python_example_group', args.topic, process_data)