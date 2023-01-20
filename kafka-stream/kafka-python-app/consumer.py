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



def process_data(key, data):
    df = pd.read_json(data).T
    df = df.astype(float)

    print(df.head())
    with client.write_api(write_options=ASYNCHRONOUS) as writer:
        writer.write(
            bucket=bucket,
            record=df,
            data_frame_measurement_name=key,
            data_frame_tag_columns=["symbol"],
            data_frame_field_columns=["Open", "High", "Low", "Close", "Adj Close", "Volume"]
            )

if __name__ == '__main__':
    #InfluxDB connection
    bucket = "DSI_test"
    #token ="yxk8_Or5qHZJxrhJE3SkAnTQSViQCsmrUoR0xPZd_0scy1T8FTuL1cKSTDKh1ft8Bqs3Zbt7Rwkys-FzajIVFQ=="
    token ="whVmtiLViagPA8zCpz4-ItfX56GPhoGMUg4s9u-kx7fXmZTNcVE9xWNbLoTXB0c347vMG8vUxxIAKDPJdsFO6A=="
    org="dsi"
    #client = InfluxDBClient(url="http://localhost:8086", token=token, org=org)
    client = InfluxDBClient(url="http://172.17.0.3:8086", token=token, org=org)




    # Read arguments and configurations and initialize
    args = ccloud_lib.parse_args()
    conf = ccloud_lib.read_ccloud_config(args.config_file)

    # Create Consumer instance
    consumer_conf = ccloud_lib.pop_schema_registry_params_from_config(conf)
    run_consumer(conf, 'python_example_group', args.topic, process_data)