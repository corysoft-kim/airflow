from airflow import DAG
from airflow.operators.python import PythonOperator
from kafka import KafkaConsumer
from datetime import datetime

# Kafka에서 메시지를 소비하는 함수 정의
def consume_messages_from_kafka():
    consumer = KafkaConsumer(
        'my_topic',  # 소비할 Kafka 주제
        bootstrap_servers='localhost:9092',  # Kafka 브로커 주소
        auto_offset_reset='earliest',  # 처음부터 메시지를 읽기 위한 설정
        group_id='my_group',  # 소비자 그룹 ID
        max_poll_records=10,  # 한 번에 소비할 최대 메시지 수
    )

    for message in consumer:
        print(f"Consumed message: {message.value.decode('utf-8')}")
        # 필요에 따라 메시지를 처리하는 로직 추가

    consumer.close()  # 소비자 종료

# 기본 설정
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 10, 18),
}

# DAG 정의
with DAG('kafka_consumer_example',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    # PythonOperator 정의
    consume_message = PythonOperator(
        task_id='consume_message',
        python_callable=consume_messages_from_kafka,  # 메시지를 소비할 함수
    )

    # 태스크 실행
    consume_message
