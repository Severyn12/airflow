from airflow import DAG
import json
from datetime import datetime
from airflow.models import Variable
from airflow.operators.sqlite_operator import SqliteOperator
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.python import PythonOperator


city_coords = {'Lviv': (49.8397, 24.0297), 'Kyiv': (50.4501, 30.5234), 
			   'Kharkiv': (49.9935, 36.2304), 'Odesa': (46.4694, 30.7409),
			   'Zhmerynka': (49.0391, 28.1086)}

tasks = []

def _process_weather(ti, **kwargs):
		task_id = kwargs['task'].upstream_task_ids
		info = ti.xcom_pull(task_id)[0]['data'][0]

		dt = info["dt"]
		temp = info["temp"]
		humidity = info["humidity"]
		clouds = info["clouds"]
		wind_speed = info["wind_speed"]

		return dt, temp, clouds, humidity, wind_speed

with DAG(dag_id='first_dag', schedule_interval='@daily', start_date=datetime(2023, 11, 23	), catchup=True) as dag:

	db_create = SqliteOperator(
		task_id="create_table_sqlite",
		sqlite_conn_id="airflow_conn",
		sql="""
			CREATE TABLE IF NOT EXISTS weather_data 
			(
			dt TIMESTAMP,
			temp FLOAT,
			clouds FLOAT,
			humidity FLOAT,
			wind_speed FLOAT
			);""")
	
		
	check_api = HttpSensor(
		task_id="check_api",
		http_conn_id="weather_conn",
		endpoint="data/3.0/onecall",
		request_params={"appid": Variable.get("WEATHER_API_KEY"), "lat": city_coords["Lviv"][0], "lon": city_coords["Lviv"][1]})


	for city, coords in city_coords.items():

		extract_data = SimpleHttpOperator(
			task_id=f"extract_data_{city.lower()}",
			http_conn_id="weather_conn",
			endpoint="data/3.0/onecall/timemachine",  
			data={"appid": Variable.get("WEATHER_API_KEY"), "lat": coords[0], "lon": coords[1], "dt": '{{ execution_date.int_timestamp }}'},
			method="GET",
			response_filter=lambda x: json.loads(x.text),
			log_response=True)
		
		process_data = PythonOperator(
			task_id=f"process_data_{city.lower()}",
			provide_context=True,
			python_callable=_process_weather
		)

		inject_data = SqliteOperator(
		task_id=f"inject_data_{city.lower()}",
		params={'process_task_id': f"process_data_{city.lower()}"},
		sqlite_conn_id="airflow_conn",
		sql="""
		INSERT INTO weather_data (dt, temp, clouds, humidity, wind_speed) VALUES 
		({{ti.xcom_pull(task_ids=task.params.process_task_id)[0]}}, 
		{{ti.xcom_pull(task_ids=task.params.process_task_id)[1]}},
		{{ti.xcom_pull(task_ids=task.params.process_task_id)[2]}},
		{{ti.xcom_pull(task_ids=task.params.process_task_id)[3]}},
		{{ti.xcom_pull(task_ids=task.params.process_task_id)[4]}});
		""")

		tasks.extend([extract_data, process_data, inject_data])

tasks.insert(0, check_api)
tasks.insert(0, db_create)

for i in range(1, len(tasks)):
    tasks[i-1] >> tasks[i]