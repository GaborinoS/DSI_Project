from confluent_kafka import Producer, KafkaError
import json
import utils.ccloud_lib as ccloud_lib
import requests
from string import Template
import pandas as pd
import time

symbols = ['IBM']

url = Template('https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=$symbol&interval=5min&apikey=5LZM13OZQMZFPU92')

def create_message(symbol):
     record_key = symbol
     r = requests.get(url.substitute(symbol=symbol))
     record_value = str(r.json())
     return { "record_key": record_key, "record_value": record_value}

# Optional per-message on_delivery handler (triggered by poll() or flush())
# when a message has been successfully delivered or
# permanently failed delivery (after retries).
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
    args = ccloud_lib.parse_args()
    producer_conf = ccloud_lib.read_ccloud_config(args.config_file)
    # Create topic if needed
    ccloud_lib.create_topic(producer_conf, args.topic)
    producer = Producer(producer_conf)

    msg = create_message('IBM')
    #data = json.loads(msg["record_value"])
    producer.produce(args.topic, key=msg["record_key"], value=msg["record_value"], on_delivery=acked)
    # #     # p.poll() serves delivery reports (on_delivery)
    # #     # from previous produce() calls.
    producer.poll(0)
    # #print(msg) 

    producer.flush()