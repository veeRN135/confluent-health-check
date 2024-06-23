import random
import string
import requests
import socket
from confluent_kafka import Producer, Consumer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient, schema_registry

# Configuration
bootstrap_servers = 'localhost:9092'
schema_registry_url = 'https://localhost:8081'
connect_cluster_url = 'https://localhost:8083'
ksql_cluster_url = 'https://localhost:8088'
servers_file_path = 'servers.txt'  # Path to the file with server names and ports

# SSL and SASL configuration
ssl_sasl_config = {
    'bootstrap.servers': bootstrap_servers,
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': 'your_username',
    'sasl.password': 'your_password',
    'ssl.ca.location': '/path/to/ca-cert',
    'ssl.certificate.location': '/path/to/client-cert',
    'ssl.key.location': '/path/to/client-key'
}

# REST API mTLS and Basic Authentication Configuration
cert = ('/path/to/client-cert', '/path/to/client-key')
verify = '/path/to/ca-cert'
auth = ('api_username', 'api_password')

def create_topic(topic_name):
    admin_client = AdminClient(ssl_sasl_config)
    new_topic = NewTopic(topic_name, num_partitions=1, replication_factor=1)
    try:
        admin_client.create_topics([new_topic])
        return 'Successful'
    except Exception as e:
        return f'Failed: {str(e)}'

def produce_message(topic_name):
    producer = Producer(ssl_sasl_config)
    message_value = 'test-message-value'
    try:
        producer.produce(topic_name, key='key', value=message_value)
        producer.flush()
        return 'Successful', message_value
    except KafkaException as e:
        return f'Failed: {str(e)}', None

def consume_message(topic_name, expected_value):
    consumer = Consumer({
        **ssl_sasl_config,
        'group.id': 'test_group',
        'auto.offset.reset': 'earliest'
    })
    consumer.subscribe([topic_name])
    try:
        msg = consumer.poll(timeout=10.0)
        if msg is None:
            return 'Failed: No message consumed'
        if msg.error():
            return f'Failed: {msg.error()}'
        partition = msg.partition()
        offset = msg.offset()
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
        'url': schema_registry_url,
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

def check_connect_cluster():
    try:
        response = requests.get(f'{connect_cluster_url}/connectors', auth=auth, cert=cert, verify=verify)
        if response.status_code == 200:
            return 'Successful'
        else:
            return f'Failed: {response.status_code}'
    except requests.RequestException as e:
        return f'Failed: {str(e)}'

def check_ksql_cluster():
    try:
        response = requests.get(f'{ksql_cluster_url}/info', auth=auth, cert=cert, verify=verify)
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
        response = requests.post(f'{connect_cluster_url}/secrets', json=secret_data, auth=auth, cert=cert, verify=verify)
        if response.status_code == 200:
            return 'Successful'
        else:
            return f'Failed: {response.status_code}'
    except requests.RequestException as e:
        return f'Failed: {str(e)}'

def retrieve_secret():
    try:
        response = requests.get(f'{connect_cluster_url}/secrets/test-secret', auth=auth, cert=cert, verify=verify)
        if response.status_code == 200:
            return 'Successful'
        else:
            return f'Failed: {response.status_code}'
    except requests.RequestException as e:
        return f'Failed: {str(e)}'

def check_server_ports(file_path):
    results = {}
    try:
        with open(file_path, 'r') as file:
            lines = file.readlines()
            for line in lines:
                server, port = line.strip().split(':')
                port = int(port)
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                    result = sock.connect_ex((server, port))
                    if result == 0:
                        results[f'{server}:{port}'] = 'Listening'
                    else:
                        results[f'{server}:{port}'] = 'Not Listening'
        return 'Successful', results
    except Exception as e:
        return f'Failed: {str(e)}', None

# Generate random topic name
topic_name = 'test-' + ''.join(random.choices(string.ascii_lowercase + string.digits, k=6))

# Perform tests
create_topic_result = create_topic(topic_name)
produce_message_result, produced_value = produce_message(topic_name)
consume_message_result = consume_message(topic_name, produced_value)
register_schema_result = register_schema(topic_name)
check_connect_cluster_result = check_connect_cluster()
check_ksql_cluster_result = check_ksql_cluster()
register_secret_result = register_secret()
retrieve_secret_result = retrieve_secret()
check_server_ports_result, server_ports_status = check_server_ports(servers_file_path)

# Report results
results = {
    'Create Topic': create_topic_result,
    'Produce Message': produce_message_result,
    'Consume Message': consume_message_result,
    'Register Schema': register_schema_result,
    'Check Connect Cluster': check_connect_cluster_result,
    'Check KSQL Cluster': check_ksql_cluster_result,
    'Register Secret': register_secret_result,
    'Retrieve Secret': retrieve_secret_result,
    'Check Server Ports': check_server_ports_result
}

for test, result in results.items():
    print(f'{test}: {result}')

if server_ports_status:
    print('Server Ports Status:')
    for server_port, status in server_ports_status.items():
        print(f'{server_port}: {status}')
