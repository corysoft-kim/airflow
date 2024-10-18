from airflow import DAG
from airflow.providers.apache.kafka.sensors.kafka import KafkaSensor
from airflow.providers.apache.kafka.operators.consumer import KafkaConsumerOperator
from datetime import datetime

# 기본 설정
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 10, 18),
}

# DAG 정의
with DAG('kafka_sensor_example',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    # KafkaSensor 정의
    wait_for_message = KafkaSensor(
        task_id='wait_for_message',
        topic='my_topic',  # 감지할 Kafka 주제
        kafka_conn_id='kafka_default',  # Airflow의 Kafka 연결 ID
        timeout=600,  # 센서가 기다릴 최대 시간 (초)
        poke_interval=10,  # 센서가 확인하는 간격 (초)
    )

    # 메시지를 소비하는 작업 정의
    consume_message = KafkaConsumerOperator(
        task_id='consume_message',
        topic='my_topic',  # 메시지를 소비할 Kafka 주제
        kafka_conn_id='kafka_default',  # Airflow의 Kafka 연결 ID
        group_id='my_group',  # 소비자 그룹 ID
        max_messages=10,  # 소비할 최대 메시지 수
        auto_offset_reset='earliest',  # 처음부터 메시지를 읽기 위한 설정
    )

    # 태스크 실행 순서 정의
    wait_for_message >> consume_message
