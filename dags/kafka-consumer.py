from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from confluent_kafka import Consumer, KafkaError

# Kafka 소비자 함수
def consume_messages():
    # Kafka 소비자 설정
    conf = {
        'bootstrap.servers': 'kafka:9092',  # Kafka 브로커 주소
        'group.id': 'my_group',                  # 소비자 그룹 ID
        'auto.offset.reset': 'earliest'          # 처음부터 읽기
    }
    
    # 소비자 생성
    consumer = Consumer(conf)
    
    # 구독할 주제 설정
    topic = 'my_topic'
    consumer.subscribe([topic])
    
    # 메시지를 소비하는 루프 (예: 5초 동안 메시지를 소비)
    try:
        for _ in range(5):  # 5번 반복
            msg = consumer.poll(1.0)  # 1초 대기
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # 마지막 메시지에 도달한 경우
                    continue
                else:
                    print(f'Consumer error: {msg.error()}')
                    break
            # 메시지 출력
            print(f'Received message: {msg.value().decode("utf-8")}')
    finally:
        consumer.close()

# DAG 정의
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 10, 19),
    'retries': 1,
}

dag = DAG(
    'kafka_consumer_dag',
    default_args=default_args,
    schedule_interval='@once',  # 한 번만 실행
)

# 메시지 소비 작업 정의
consume_task = PythonOperator(
    task_id='consume_kafka_messages',
    python_callable=consume_messages,
    dag=dag,
)

# DAG에 작업 추가
consume_task
