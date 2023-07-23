from datetime import datetime, timedelta
import json
from kafka import KafkaProducer
from airflow import DAG, settings
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Connection
from airflow.utils.dates import days_ago


def create_conn(conn_id, conn_type, host):
    conn = Connection(conn_id=conn_id, conn_type=conn_type, host=host)
    session = settings.Session()
    conn_name = (
        session.query(Connection).filter(Connection.conn_id == conn.conn_id).first()
    )
    if str(conn_name) == str(conn.conn_id):  # check if the connection already exists
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


def push_to_kafka(station_status, station_information):
    producer = KafkaProducer(
        bootstrap_servers="kafka:9092",
        key_serializer=lambda v: json.dumps(v).encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    data_station_status = json.loads(station_status)
    data_station_information = json.loads(station_information)
    producer.send(
        "station_status",
        key=data_station_status["lastUpdatedOther"],
        value=data_station_status["data"]["stations"],
    )
    producer.send(
        "station_information",
        key=data_station_information["lastUpdatedOther"],
        value=data_station_information["data"]["stations"],
    )


with DAG(
    dag_id="VelibServiceCall",
    description="This dag is to get data from weather api",
    start_date=datetime(2023, 7, 18),
    catchup=False,
    schedule_interval=timedelta(minutes=5),
) as dag:
    create_conn("velib_api", "http", "https://velib-metropole-opendata.smoove.pro")
    task_station_status = SimpleHttpOperator(
        task_id="velib_station_status",
        method="GET",
        http_conn_id="velib_api",
        endpoint="/opendata/Velib_Metropole/station_status.json",
        headers={"Content-type": "application/json"},
        response_check=lambda response: handle_response(response),
        do_xcom_push=True,
        dag=dag,
    )
    task_station_information = SimpleHttpOperator(
        task_id="velib_station_information",
        method="GET",
        http_conn_id="velib_api",
        endpoint="/opendata/Velib_Metropole/station_information.json",
        headers={"Content-type": "application/json"},
        response_check=lambda response: handle_response(response),
        do_xcom_push=True,
        dag=dag,
    )
    task_kafka_producer = PythonOperator(
        task_id="push_to_kafka",
        python_callable=push_to_kafka,
        op_args=[
            "{{ ti.xcom_pull(task_ids='velib_station_status') }}",
            "{{ ti.xcom_pull(task_ids='velib_station_information') }}",
        ],
        dag=dag,
    )

    [task_station_status, task_station_information] >> task_kafka_producer
