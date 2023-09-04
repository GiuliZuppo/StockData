import os
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
import psycopg2
import smtplib
from airflow.operators.email_operator import EmailOperator


# Configurar los argumentos por defecto para el DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 8, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Crear el DAG
dag = DAG(
    "stock_data_ingestion",
    default_args=default_args,
    schedule_interval=timedelta(days=1),
)

def verificar_condicion_alerta(df):
    limite_alerta_volumen = 30
    ultimo_volumen = df.iloc[-1]['volume']
    
    if ultimo_volumen < limite_alerta_volumen:
        return "El volumen de la acción ha caído por debajo del límite de alerta: {0}".format(ultimo_volumen)
    else:
        return None  # No se envía correo electrónico si no se cumple la condición


df = None

# Definir la función para la tarea de ingestión de datos
def ingest_data_function():
    # Obtener credenciales de las variables de entorno
    rapidapi_key = os.environ.get("RAPIDAPI_KEY")
    redshift_host = os.environ.get("REDSHIFT_HOST")
    redshift_port = os.environ.get("REDSHIFT_PORT")
    redshift_dbname = os.environ.get("REDSHIFT_DBNAME")
    redshift_user = os.environ.get("REDSHIFT_USER")
    redshift_password = os.environ.get("REDSHIFT_PASSWORD")

    # Solicitud a la API y crear el DataFrame
    url = "https://alpha-vantage.p.rapidapi.com/query"
    querystring = {
        "interval": "5min",
        "function": "TIME_SERIES_INTRADAY",
        "symbol": "MSFT",
        "datatype": "json",
        "output_size": "compact"
    }
    headers = {
        "X-RapidAPI-Key": rapidapi_key,
        "X-RapidAPI-Host": "alpha-vantage.p.rapidapi.com"
    }
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        data_json = response.json()
        time_series_data = data_json.get("Time Series (5min)", {})
        df = pd.DataFrame.from_dict(time_series_data, orient="index")
        df.index.name = 'Fecha'

        #Verificar la condicion de alerta
        mensaje_alerta = verificar_condicion_alerta(df)
        
        # Conectar a la base de datos Redshift
        try:
            connection = psycopg2.connect(
                host=redshift_host,
                port=redshift_port,
                dbname=redshift_dbname,
                user=redshift_user,
                password=redshift_password
            )
            cursor = connection.cursor()

            # Crear tabla en Redshift (si aún no existe)
            create_table_query = """
            CREATE TABLE IF NOT EXISTS stock_data (
                fecha TIMESTAMP,
                open NUMERIC,
                high NUMERIC,
                low NUMERIC,
                close NUMERIC,
                volume INT
            );
            """
            cursor.execute(create_table_query)
            connection.commit()

            # Cargar datos en la tabla
            for index, row in df.iterrows():
                insert_query = """
                INSERT INTO stock_data (fecha, open, high, low, close, volume)
                VALUES (%s, %s, %s, %s, %s, %s);
                """
                cursor.execute(insert_query, (
                    index,
                    row['1. open'],
                    row['2. high'],
                    row['3. low'],
                    row['4. close'],
                    row['5. volume']
                ))
            connection.commit()

            # Si hay una alerta, enviar un correo electrónico
            if mensaje_alerta:
                send_alert_task = EmailOperator(
                    task_id="send_alert_task",
                    to=os.environ.get("AIRFLOW_EMAIL_SENDER"),
                    subject="Alerta de volumen de acción",
                    html_content=mensaje_alerta,
                    dag=dag,
                )


        except (Exception, psycopg2.Error) as error:
            print(f"Error al conectar o cargar datos en Redshift: {error}")
        finally:
            # Cerrar la conexión a la base de datos
            if connection:
                cursor.close()
                connection.close()
    else:
        print(f"Error al hacer la solicitud a la API. Código de estado: {response.status_code}")
        exit()

# Crear la tarea que ejecuta la función de ingestión de datos
ingest_data_task = PythonOperator(
    task_id="ingest_data_task",
    python_callable=ingest_data_function,
    dag=dag,
)




