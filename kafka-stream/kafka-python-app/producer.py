from confluent_kafka import Producer, KafkaError

import json
import utils.ccloud_lib as ccloud_lib
import requests
from string import Template
import pandas as pd
import time
#Data Source
import yfinance as yf
from psycopg2.extras import Json
import psycopg2
 
def create_message(symbol, data):
     record_key = symbol
     record_value = data
     return { "record_key": record_key, "record_value": record_value}

# url = Template('https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=$symbol&interval=5min&apikey=5LZM13OZQMZFPU92')

# def create_message(symbol):
#      record_key = symbol
#      r = requests.get(url.substitute(symbol=symbol))
#      record_value = str(r.json())
#      return { "record_key": record_key, "record_value": record_value}

# Optional per-message on_delivery handler (triggered by poll() or flush())
# when a message has been successfully delivered or
# permanently failed delivery (after retries).

def get_ticker_symbols():
    #-----Password hidden
    f=open("PW.txt", "r") 
    userPw=[f.readline()]
    f.close()
    #------------------------SQL Connection

    conn = psycopg2.connect(host="mds-dsi-db.postgres.database.azure.com",
                            port="5432",
                            database="finance_data",
                            user="ds22m017",
                            password=str(userPw[0]),
                            connect_timeout=3)
    cur = conn.cursor()

    print("con done")

    #------------------------SQL Query
    query = "SELECT * FROM wiki_sp_500_companies"
    SP_500 = pd.read_sql_query(query, conn)

    print("query done")

    #------------------------SQL Close
    cur.close()
    conn.close()
    return SP_500['symbol']

def acked(err, msg):
    """Delivery report handler called on
    successful or failed delivery of message
    """
    if err is not None:
        print("Failed to deliver message: {}".format(err))
    else:
        print("Produced record to topic {} partition [{}] @ offset {}"
                .format(msg.topic(), msg.partition(), msg.offset()))

if __name__ == '__main__':
    while(True):
        args = ccloud_lib.parse_args()
        producer_conf = ccloud_lib.read_ccloud_config(args.config_file)
        # Create topic if needed
        ccloud_lib.create_topic(producer_conf, args.topic)
        producer = Producer(producer_conf)

        symbols = get_ticker_symbols()

        for symbol in symbols:
            #Interval required 1 minute
            data = yf.download(tickers=symbol, period='1d', interval='1m').to_json(orient = "index")

            msg = create_message(symbol, data)
            #data = json.loads(msg["record_value"])
            producer.produce(args.topic, key=msg["record_key"], value=msg["record_value"], on_delivery=acked)
            producer.poll(0)
            producer.flush()

        #suspend for 2 days
        time.sleep(172800)