from collections import namedtuple
import os
class EnviornmentVariables:
    
    def __init__(self):
        cassandra=[]
        cassandra.append(os.getenv('BUNDLE_ZIP'))
        cassandra.append(os.getenv('TOKEN_JSON'))
        self.cassandra_connect = namedtuple('cassandra_connect',['bundle_connect','token_json'])(*cassandra)

        cassandra_input =[]
        cassandra_input.append(os.getenv('REPLICATION_FACTOR'))
        cassandra_input.append(os.getenv('TABLE_NAME'))
        cassandra_input.append(os.getenv('CLASS'))
        cassandra_input.append(os.getenv('KEYSPACE'))
        self.cassandra_input = namedtuple('cassandra_input', ['replication_factor','table_name', 'classes','keyspace'])(* cassandra_input)

        kafka_cloud = []
        kafka_cloud.append(os.getenv('API_KEY'))
        kafka_cloud.append(os.getenv('API_SECRET_KEY'))
        kafka_cloud.append(os.getenv('BOOTSTRAP_SERVER'))
        kafka_cloud.append(os.getenv('SECURITY_PROTOCOL'))
        kafka_cloud.append(os.getenv('SSL_MECHANISM'))
        kafka_cloud.append(os.getenv('ENDPOINT_URL'))
        kafka_cloud.append(os.getenv('AUTO_OFFSET_RESET'))
        kafka_cloud.append(os.getenv('SCHEMA_REGISTRY_API_KEY'))
        kafka_cloud.append(os.getenv('SCHEMA_REGISTRY_API_SECRET_KEY'))
        self.kafka_cloud = namedtuple('kafka_cloud',["api_key","api_secret_key",'bootstrap_server','security_protocol','ssl_mechanism','endpoint_url','auto_offset_reset','schema_registry_api_key','schema_registry_secret_key'])(*kafka_cloud)
        
