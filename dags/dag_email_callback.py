from datetime import datetime, timedelta
import pandas as  pd
from scripts.functions import connect_to_db, build_conn_string
from airflow.models import DAG, Variable
from airflow.operators.python import PythonOperator

import smtplib

config_dir = "config/config.ini"
conn_str = build_conn_string(config_dir, "redshift")
conn, engine = connect_to_db(conn_str)

default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=1)
}

def busco_artista():
    artista = pd.read_sql_query("select artist_name, max(updated_at) from guilleale22_coderhouse.artistas_top_50_global where artist_name = 'Taylor Swift' group by 1", conn)
    if artista['artist_name'][0] == 'Taylor Swift':
        return artista['artist_name'][0]
    else :
        return 'Taylor no esta en los 50 principales de hoy'

artista_encontrado = busco_artista()

def enviar_status(context,):
    try:
        x = smtplib.SMTP('smtp.gmail.com',587)
        x.starttls()
        
        print(Variable.get('GMAIL_SECRET'))
        x.login(
            'guilleale22@gmail.com',
            Variable.get('GMAIL_SECRET')
        )
        subject = f'Airflow reporte {context["dag"]} {context["ds"]}'
        task_instance = context['task_instance']
        task_status = task_instance.current_state()
        #body_text = f'Tarea {context["task_instance_key_str"]} ejecutada exitosamente'
        body_text =  f"The task {task_instance.task_id} finished with status: {task_status}.\n\n" \
           f"Task execution date: {context['execution_date']}\n" \
           f"Log URL: {task_instance.log_url}\n\n"\
           f"Artista encontrado : {artista_encontrado}\n\n"
        #body_text = context.get('exception')
        message='Subject: {}\n\n{}'.format(subject,body_text)
        
        x.sendmail('guilleale22@gmail.com','guilleale22@gmail.com',message)
        print('Exito')
    except Exception as exception:
        print(exception)
        print('Failure')

with DAG( 
    dag_id='dag_smtp_email_callback',
    schedule_interval="0 9 * * *",
    catchup=False,
    default_args=default_args,
    on_success_callback=enviar_status,
    on_failure_callback=enviar_status,
    start_date=datetime(2023, 11, 28)
):
    tarea_1 = PythonOperator(
        task_id='busco_artista',
        python_callable= busco_artista
        )