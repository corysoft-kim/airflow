from airflow import DAG
from airflow.operators.python import PythonOperator
from kafka import KafkaProducer
from datetime import datetime

# Kafka에 메시지를 전송하는 함수 정의
def send_message_to_kafka():
    producer = KafkaProducer(bootstrap_servers='localhost:9092')  # Kafka 브로커 주소 설정
    producer.send('my_topic', value=b'hello world')  # 메시지 전송
    producer.flush()  # 메시지 전송 완료
    producer.close()  # 프로듀서 종료

# 기본 설정
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 10, 18),
}

# DAG 정의
with DAG('kafka_producer_example',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    # PythonOperator 정의
    send_message = PythonOperator(
        task_id='send_message',
        python_callable=send_message_to_kafka,  # 메시지를 전송할 함수
    )

    # 태스크 실행
    send_message
