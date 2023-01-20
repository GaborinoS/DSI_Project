from datetime import datetime
from utils.kafka_connector import run_consumer
from utils import ccloud_lib as ccloud_lib

def process_data(message):
    print(message)

if __name__ == '__main__':
    # Read arguments and configurations and initialize
    args = ccloud_lib.parse_args()
    conf = ccloud_lib.read_ccloud_config(args.config_file)

    # Create Consumer instance
    consumer_conf = ccloud_lib.pop_schema_registry_params_from_config(conf)
    run_consumer(conf, 'es_group', args.topic, process_data)