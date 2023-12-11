from airflow import DAG
import os
import sqlite3
from google.oauth2 import service_account
from datetime import datetime
from transformers import pipeline
from airflow.models import Variable
from airflow.operators.sqlite_operator import SqliteOperator
from google.cloud import vision
from airflow.operators.python import PythonOperator



def retrieve_domains():
    conn = sqlite3.connect(Variable.get("SQLITE_DB_HOST"))
    cursor = conn.cursor()

    cursor.execute("SELECT domain FROM DOMAIN_DATA")
    existing_domains = [row[0] for row in cursor.fetchall()]

    conn.close()
    return existing_domains

def domain_exists(new_domain, existing_domains):
    return new_domain in existing_domains

def inject_domain_data(ti):
        
    existing_domains = retrieve_domains()
    scrapped_domains = ti.xcom_pull(task_ids='ocr_imgs', key='domains')

    conn = sqlite3.connect(Variable.get("SQLITE_DB_HOST"))
    cursor = conn.cursor()
    
    for new_domain, description in scrapped_domains.items():
        if domain_exists(new_domain, existing_domains):
            cursor.execute("SELECT description FROM DOMAIN_DATA WHERE domain=?", (new_domain,))
            existing_description = cursor.fetchone()[0]
            new_description = existing_description + '\n' + description
            cursor.execute("UPDATE DOMAIN_DATA SET description=? WHERE domain=?", (new_description, new_domain))
        else:
            cursor.execute("INSERT INTO DOMAIN_DATA (domain, description) VALUES (?, ?)", (new_domain, description))

    conn.commit()
    conn.close()

def enrich_data(domens):

    generator = pipeline('text-generation', model='bert-base-uncased')
    generated_data = {}

    for domen in domens:
        input_text = f"You should describe: {domen}"
        enriched_text = generator(input_text, max_length=100, num_return_sequences=1)[0]['generated_text']
        generated_data[domen] = enriched_text.split(':')[1].strip()
        
    return generated_data

def ocr_img(ti):

    image_folder_path = Variable.get("IMG_FOLDER")
    credentials = service_account.Credentials.from_service_account_file(Variable.get("VISION_API_CREDS"))

    domains = []
    image_files = [f for f in os.listdir(image_folder_path)]
    domain_extn = ['.com', '.org', '.net', '.edu', '.gov', '.us']

    client = vision.ImageAnnotatorClient(credentials = credentials)

    for image_file in image_files:
        image_path = os.path.join(image_folder_path, image_file)

        with open(image_path, "rb") as image_file:
            content = image_file.read()

        image = vision.Image(content=content)

        response = client.text_detection(image=image)
        texts = response.text_annotations[1::]

        descriptions = [item.description for item in texts]
        for text in descriptions:
        	for extn in domain_extn:
                    if extn in text.lower():
                        domains.append(text.lower())
                        break 
    ti.xcom_push(key='domains', value=enrich_data(domains))

with DAG(dag_id='second_dag', schedule_interval='@daily', start_date=datetime(2023, 12, 10), catchup=True) as dag:

	db_create = SqliteOperator(
		task_id="create_table_sqlite",
		sqlite_conn_id="airflow_conn_domains",
		sql="""
			CREATE TABLE IF NOT EXISTS DOMAIN_DATA 
			(
			domain string,
            description string
			);""")
		
	process_data = PythonOperator(
		task_id=f"ocr_imgs",
		provide_context=True,
		python_callable= ocr_img
	)
     
	inject_data = PythonOperator(
		task_id=f"inject_data",
		provide_context=True,
		python_callable= inject_domain_data
	)

db_create >> process_data >> inject_data