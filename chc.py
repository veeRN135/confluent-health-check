import random
import string
import requests
import socket
from confluent_kafka import Producer, Consumer, KafkaException, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient, schema_registry

# Configuration for different environments
environments = {
    'dev': {
        'bootstrap_servers': 'dev-bootstrap-server:9092',
        'schema_registry_url': 'https://dev-schema-registry:8081',
        'connect_cluster_url': 'https://dev-connect-cluster:8083',
        'ksql_cluster_url': 'https://dev-ksql-cluster:8088',
        'auth': ('dev_username', 'dev_password')
    },
    'qa': {
        'bootstrap_servers': 'qa-bootstrap-server:9092',
        'schema_registry_url': 'https://qa-schema-registry:8081',
        'connect_cluster_url': 'https://qa-connect-cluster:8083',
        'ksql_cluster_url': 'https://qa-ksql-cluster:8088',
        'auth': ('qa_username', 'qa_password')
    },
    'prod': {
        'bootstrap_servers': 'prod-bootstrap-server:9092',
        'schema_registry_url': 'https://prod-schema-registry:8081',
        'connect_cluster_url': 'https://prod-connect-cluster:8083',
        'ksql_cluster_url': 'https://prod-ksql-cluster:8088',
        'auth': ('prod_username', 'prod_password')
    }
}

# Set the environment
environment = 'dev'  # Change this to 'qa' or 'prod' as needed

# Get configuration for the selected environment
config = environments[environment]

# SSL and SASL configuration (certificates are the same across environments)
ssl_sasl_config = {
    'bootstrap.servers': config['bootstrap_servers'],
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': config['auth'][0],
    'sasl.password': config['auth'][1],
    'ssl.ca.location': '/path/to/ca-cert',
    'ssl.certificate.location': '/path/to/client-cert',
    'ssl.key.location': '/path/to/client-key'
}

# REST API mTLS and Basic Authentication Configuration
cert = ('/path/to/client-cert', '/path/to/client-key')
verify = '/path/to/ca-cert'
auth = config['auth']

def create_topic(topic_name):
    admin_client = AdminClient(ssl_sasl_config)
    new_topic = NewTopic(topic_name, num_partitions=1, replication_factor=1)
    futures = admin_client.create_topics([new_topic])

    if topic_name in futures:
        try:
            future = futures[topic_name]
            future.result()  # Block until the topic creation is complete or raises an exception
            return 'Successful'
        except Exception as e:
            return f'Failed: {str(e)}, {type(e).__name__}'
    else:
        return f'Failed: Topic {topic_name} not found in futures'

def produce_message(topic_name):
    producer = Producer(ssl_sasl_config)
    message_value = 'test-message-value'
    delivery_info = {}

    def delivery_report(err, msg):
        if err is not None:
            delivery_info['error'] = str(err)
        else:
            delivery_info['partition'] = msg.partition()
            delivery_info['offset'] = msg.offset()

    try:
        producer.produce(topic_name, key='key', value=message_value, callback=delivery_report)
        producer.flush()
        if 'error' in delivery_info:
            return f'Failed: {delivery_info["error"]}', None, None
        return 'Successful', message_value, delivery_info
    except KafkaException as e:
        return f'Failed: {str(e)}', None, None
    except Exception as e:
        return f'Failed: {str(e)}', None, None

def consume_message(topic_name, partition, offset, expected_value):
    consumer = Consumer({
        **ssl_sasl_config,
        'group.id': 'test_group',
        'auto.offset.reset': 'earliest'
    })
    consumer.assign([TopicPartition(topic_name, partition, offset)])
    try:
        msg = consumer.poll(timeout=10.0)
        if msg is None:
            return 'Failed: No message consumed'
        if msg.error():
            return f'Failed: {msg.error()}'
        consumed_value = msg.value().decode('utf-8')
        if consumed_value == expected_value:
            return f'Successful: Partition: {partition}, Offset: {offset}, Message: {consumed_value}'
        else:
            return f'Failed: Message mismatch. Expected: {expected_value}, Consumed: {consumed_value}'
    except KafkaException as e:
        return f'Failed: {str(e)}'
    finally:
        consumer.close()

def register_schema(topic_name):
    schema_registry_client = SchemaRegistryClient({
        'url': config['schema_registry_url'],
        'ssl.ca.location': '/path/to/ca-cert',
        'ssl.certificate.location': '/path/to/client-cert',
        'ssl.key.location': '/path/to/client-key'
    })
    schema_str = '{"type": "string"}'  # Simple Avro schema for a string type
    try:
        schema_id = schema_registry_client.register(f'{topic_name}-value', schema_registry.AvroSchema(schema_str))
        return 'Successful'
    except Exception as e:
        return f'Failed: {str(e)}'

def retrieve_schema(topic_name):
    schema_registry_client = SchemaRegistryClient({
        'url': config['schema_registry_url'],
        'ssl.ca.location': '/path/to/ca-cert',
        'ssl.certificate.location': '/path/to/client-cert',
        'ssl.key.location': '/path/to/client-key'
    })
    try:
        schema = schema_registry_client.get_latest_version(f'{topic_name}-value')
        return f'Successful: {schema.schema.schema_str}'
    except Exception as e:
        return f'Failed: {str(e)}'

def check_connect_cluster():
    try:
        response = requests.get(f'{config["connect_cluster_url"]}/connectors', auth=auth, cert=cert, verify=verify)
        if response.status_code == 200:
            return 'Successful'
        else:
            return f'Failed: {response.status_code}'
    except requests.RequestException as e:
        return f'Failed: {str(e)}'

def check_ksql_cluster():
    try:
        response = requests.get(f'{config["ksql_cluster_url"]}/info', auth=auth, cert=cert, verify=verify)
        if response.status_code == 200:
            return 'Successful'
        else:
            return f'Failed: {response.status_code}'
    except requests.RequestException as e:
        return f'Failed: {str(e)}'

def register_secret():
    secret_data = {
        "name": "test-secret",
        "value": "s3cr3t"
    }
    try:
        response = requests.post(f'{config["connect_cluster_url"]}/secrets', json=secret_data, auth=auth, cert=cert, verify=verify)
        if response.status_code == 200:
            return 'Successful'
        else:
            return f'Failed: {response.status_code}'
    except requests.RequestException as e:
        return f'Failed: {str(e)}'

def retrieve_secret():
    try:
        response = requests.get(f'{config["connect_cluster_url"]}/secrets/test-secret', auth=auth, cert=cert, verify=verify)
        if response.status_code == 200:
            return 'Successful'
        else:
            return f'Failed: {response.status_code}'
    except requests.RequestException as e:
        return f'Failed: {str(e)}'

def check_server_ports(file_path):
    results = {}
    context = ssl.create_default_context()
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    try:
        with open(file_path, 'r') as file:
            lines = file.readlines()
            for line in lines:
                server, port = line.strip().split(':')
                port = int(port)
                try:
                    with socket.create_connection((server, port)) as sock:
                        with context.wrap_socket(sock, server_hostname=server) as ssock:
                            results[f'{server}:{port}'] = 'Listening'
                except Exception as e:
                    results[f'{server}:{port}'] = f'Failed: {str(e)}'
        return 'Successful', results
    except Exception as e:
        return f'Failed: {str(e)}', None

# Generate random topic name
topic_name = 'test-' + ''.join(random.choices(string.ascii_lowercase + string.digits, k=6))

# Perform tests
create_topic_result = create_topic(topic_name)
produce_message_result, produced_value, delivery_info = produce_message(topic_name)

if delivery_info:
    partition = delivery_info['partition']
    offset = delivery_info['offset']
    consume_message_result = consume_message(topic_name, partition, offset, produced_value)
else:
    consume_message_result = 'Failed: No delivery information available'

register_schema_result = register_schema(topic_name)
retrieve_schema_result = retrieve_schema(topic_name)
check_connect_cluster_result = check_connect_cluster()
check_ksql_cluster_result = check_ksql_cluster()
register_secret_result = register_secret()
retrieve_secret_result = retrieve_secret()
check_server_ports_result, server_ports_status = check_server_ports('/path/to/servers_file')

# Report results
results = {
    'Create Topic': create_topic_result,
    'Produce Message': produce_message_result,
    'Consume Message': consume_message_result,
    'Register Schema': register_schema_result,
    'Retrieve Schema': retrieve_schema_result,
    'Check Connect Cluster': check_connect_cluster_result,
    'Check KSQL Cluster': check_ksql_cluster_result,
    'Register Secret': register_secret_result,
    'Retrieve Secret': retrieve_secret_result,
    'Check Server Ports': check_server_ports_result
}

# Print results
for test, result in results.items():
    print(f'{test}: {result}')

if server_ports_status:
    print('Server Ports Status:')
    for server, status in server_ports_status.items():
        print(f'{server_port}: {status}')
