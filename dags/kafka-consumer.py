from airflow import DAG
from airflow.providers.apache.kafka.operators.consumer import KafkaConsumerOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 10, 18),
}

with DAG('kafka_consumer_example', default_args=default_args, schedule_interval='@daily') as dag:
    consume_message = KafkaConsumerOperator(
        task_id='consume_message',
        topic='my_topic',
        kafka_conn_id='test-id',
        group_id='my_group',  # 소비자 그룹 ID 설정
        max_messages=100,  # 소비할 최대 메시지 수 설정
        auto_offset_reset='earliest',  # 메시지를 처음부터 읽기 위한 설정
    )
