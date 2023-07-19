from datetime import datetime, timedelta
import json
from kafka import KafkaProducer
from airflow import DAG,settings
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Connection
from airflow.utils.dates import days_ago

def create_conn(conn_id, conn_type, host):
    conn = Connection(conn_id=conn_id,
                      conn_type=conn_type,
                      host=host)
    session = settings.Session()
    conn_name = session.query(Connection).filter(Connection.conn_id == conn.conn_id).first()
    if str(conn_name) == str(conn.conn_id):#check if the connection already exists
        return None
    session.add(conn)
    session.commit()
    return conn


def handle_response(response):
    if response.status_code == 200:
        print("Received 200 OK")
        return True
    else:
        print("error")
        return False
        
def push_to_kafka(ti):
    producer = KafkaProducer(bootstrap_servers='kafka:9092',key_serializer=lambda v: json.dumps(v).encode('utf-8'),value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    data = json.loads(ti.xcom_pull(task_ids='velib_api'))
    producer.send("myTopic",key=data['lastUpdatedOther'],value= data['data'])

with DAG(
    dag_id='VelibServiceCall',
    description='This dag is to get data from weather api',
    start_date= datetime(2023, 7, 18),
    catchup=False,
    schedule_interval= timedelta(minutes=5)
)as dag:
    create_conn("velib_api","http","https://velib-metropole-opendata.smoove.pro")
    task1 = SimpleHttpOperator(
        task_id='velib_api',
        method='GET',
        http_conn_id='velib_api',
        endpoint='/opendata/Velib_Metropole/station_status.json',
        headers={"Content-type":"application/json"},
        response_check=lambda response:handle_response(response),
        do_xcom_push=True,
        dag=dag
    )
    task2 = PythonOperator(
        task_id='push_to_kafka',
        python_callable=push_to_kafka,
        dag=dag
    )

    task1 >> [task2]

