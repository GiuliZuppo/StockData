from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
import psycopg2

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 8, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "stock_data_ingestion",
    default_args=default_args,
    schedule_interval=timedelta(days=1),  # Programa el DAG para que se ejecute diariamente
)

def ingest_data_function():
    import requests
import pandas as pd
import psycopg2

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
    "X-RapidAPI-Key": "1f26e6b29bmsh13204e57bfb7c06p19c023jsn9ebc5651627b",
    "X-RapidAPI-Host": "alpha-vantage.p.rapidapi.com"
}
response = requests.get(url, headers=headers, params=querystring)

if response.status_code == 200:
    data_json = response.json()
    time_series_data = data_json.get("Time Series (5min)", {})
    df = pd.DataFrame.from_dict(time_series_data, orient="index")
    df.index.name = 'Fecha'
else:
    print(f"Error al hacer la solicitud a la API. Código de estado: {response.status_code}")
    exit()

# Conectar a la base de datos Redshift
try:
    connection = psycopg2.connect(
        host="data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com",
        port="5439",
        dbname="data-engineer-database",
        user="giulianazuppo_coderhouse",
        password="w36txMDb20"
    )
    cursor = connection.cursor()

    # Crear la tabla en Redshift
    create_table_query = """
    CREATE TABLE IF NOT EXISTS stock_data (
        fecha TIMESTAMP,
        open_price FLOAT,
        high FLOAT,
        low FLOAT,
        close FLOAT,
        volume FLOAT
    );
    """
    cursor.execute(create_table_query)
    connection.commit()

    # Convertir el DataFrame a una lista
    data_to_insert = []
    for index, row in df.iterrows():
        row_data = (
            index,
            row.get("1. open"),
            row.get("2. high"),
            row.get("3. low"),
            row.get("4. close"),
            row.get("5. volume")
        )
        data_to_insert.append(row_data)

    # Insertar los datos en la tabla Redshift
    insert_query = """
    INSERT INTO stock_data (fecha, open_price, high, low, close, volume)
    VALUES (%s, %s, %s, %s, %s, %s);
    """
    cursor.executemany(insert_query, data_to_insert)
    connection.commit()

    print("Datos cargados en la tabla Redshift.")
except (Exception, psycopg2.Error) as error:
    print(f"Error al conectar o cargar datos en Redshift: {error}")
finally:
    # Cerrar la conexión a la base de datos
    if connection:
        cursor.close()
        connection.close()
        
ingest_data_task = PythonOperator(
    task_id="ingest_data_task",
    python_callable=ingest_data_function,
    dag=dag,
)

ingest_data_task

