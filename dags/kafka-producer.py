from airflow import DAG
from airflow.providers.apache.kafka.operators.producer import KafkaProducerOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 10, 18),
}

with DAG('kafka_example', default_args=default_args, schedule_interval='@daily') as dag:
    send_message = KafkaProducerOperator(
        task_id='send_message',
        topic='my_topic',
        value='Hello Kafka!',
        kafka_conn_id='test-id',
    )
