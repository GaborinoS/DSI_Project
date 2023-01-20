import influxdb_client
from influxdb import DataFrameClient
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
import yfinance as yf

#InfluxDB connection
bucket = "DSI_test"
#token ="yxk8_Or5qHZJxrhJE3SkAnTQSViQCsmrUoR0xPZd_0scy1T8FTuL1cKSTDKh1ft8Bqs3Zbt7Rwkys-FzajIVFQ=="
token ="GMbnHWGhM9p9t9mIjbc1I5KlWir8LJxBDkpMF0SiOa56f1nvLepCEN7iI_5-sR80FA8CvLmf_mHcy8Gc5XYwvA=="
org="dsi"
client = InfluxDBClient(url="http://localhost:8086", token=token, org=org)
#client = InfluxDBClient(url="http://172.19.0.2:8086", token=token, org=org)

data = yf.download(tickers='IBM', period='1d', interval='1m')

with client.write_api(write_options=SYNCHRONOUS) as writer:
    writer.write(
        bucket=bucket,
        record=data,
        data_frame_measurement_name="key4",
        data_frame_tag_columns=["symbol"],
        data_frame_field_columns=["Open", "High", "Low", "Close", "Adj Close", "Volume"]
        )