import streamlit as st
st.set_page_config(layout="wide")
import plotly.graph_objects as go
import pandas as pd
import numpy as np
import json
from psycopg2.extras import Json
import psycopg2



#load csv, replace by DB-query
df_input = pd.read_csv("ABBV.csv")
symbol = df_input["symbol"].unique()
df_input = df_input.pivot(index="time", columns=["field"], values = ["value"])
df_input["symbol"] = [symbol[0] for x in df_input.index]
print(df_input.head())


@st.cache
def load_data(df):
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

    data = [df, SP_500]   

    return data

def render_info():
    st.title("Finance Data")
    st.header(" ")
    st.text(" ")

render_info()

data = load_data(df_input)


df_price = data[0]
df_info = data[1] 

symbols = df_info["symbol"].unique()

leftcol, rightcol = st.columns([2, 1])

with rightcol:  
    stock = st.selectbox("Stock", symbols)

    #Candlestick Plot
    df_input = df_price[df_price["symbol"] == stock]
    fig = go.Figure(data=[go.Candlestick(x=df_input.index,
                open=df_input["value"]["Open"],
                high=df_input["value"]["High"],
                low=df_input["value"]["Low"],
                close=df_input["value"]["Close"])])
    st.plotly_chart(fig, use_container_width=False, sharing="streamlit", theme="streamlit")

        
with leftcol:
    #Infotable
    df_info_table = pd.DataFrame([df_info [df_info ["symbol"] == stock]["info"].values[0]]).T
    df_info_table.columns = [stock]
    st.table(df_info_table)








