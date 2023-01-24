import streamlit as st
st.set_page_config(layout="wide")
import plotly.graph_objects as go
import pandas as pd
import numpy as np
import json
from psycopg2.extras import Json
import psycopg2
from influxdb import DataFrameClient
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS




def data_from_influx(symbol, display_range):

    bucket = "DSI_test"
    token ="GMbnHWGhM9p9t9mIjbc1I5KlWir8LJxBDkpMF0SiOa56f1nvLepCEN7iI_5-sR80FA8CvLmf_mHcy8Gc5XYwvA=="
    org="dsi"
    client = InfluxDBClient(url="http://localhost:8086", token=token, org=org)

    query_api = client.query_api()


    query = """from(bucket: "DSI_test")
            |> range(start: -"""+display_range+""")
            |> filter(fn: (r) => r["_measurement"] ==\"""" +symbol+"""\")
            |> filter(fn: (r) => r["_field"] == "Open" or r["_field"] == "Close" or r["_field"] == "High" or r["_field"] == "Low")"""

    result = query_api.query(org=org, query=query)

    results = []
    for table in result:
        for record in table.records:
            results.append((record.get_time(), record.get_measurement(), record.get_field(), record.get_value()))

    #make a dataframe from the results

    df = pd.DataFrame(results, columns=['Date',"Symbol", 'OHLC', "Value"])
    df['Date'] = pd.to_datetime(df['Date'])
    #if OHLC is Open, High, Low, or Close, then make it a column
    df = df.pivot(index='Date', columns='OHLC', values='Value')
    
    return df



@st.cache
def load_data():
    #-----Password hidden
    f=open("PW.txt", "r") 
    userPw=[f.readline()]
    f.close()
    #------------------------SQL Connection

    conn = psycopg2.connect(host="mds-dsi-db.postgres.database.azure.com",
                            port="5432",
                            database="finance_data",
                            user="ds22m017",
                            password=str(userPw[0]),#SchachingeR14
                            connect_timeout=3)
    cur = conn.cursor()

    #------------------------SQL Query
    query = "SELECT * FROM wiki_sp_500_companies"
    SP_500 = pd.read_sql_query(query, conn)

    #------------------------SQL Close
    cur.close()
    conn.close()



    return SP_500

def render_info():
    st.title("Finance Data")
    st.header(" ")
    st.text(" ")

render_info()

data = load_data()


symbols = data["symbol"].unique()

leftcol, rightcol = st.columns([1, 1])

with rightcol:  
    stock = st.selectbox("Stock", symbols)

    #Candlestick Plot
    df_input = data_from_influx(stock, "10d")
    #df_input = df_price[df_price["symbol"] == stock]
    fig = go.Figure(data=[go.Candlestick(x=df_input.index,
                open=df_input["Open"],
                high=df_input["High"],
                low=df_input["Low"],
                close=df_input["Close"])])
    st.plotly_chart(fig, use_container_width=True, sharing="streamlit", theme="streamlit")

        
with leftcol:
    #Infotable
    df_info_table = pd.DataFrame([data [data ["symbol"] == stock]["info"].values[0]]).T
    df_info_table.columns = [stock]
    st.table(df_info_table)








