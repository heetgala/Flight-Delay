from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import pyodbc

# ✅ Static paths (adjust if needed)
CSV_PATH = "/opt/airflow/data/Airline_Delay_Cause.csv"

# ✅ SQL Server connection details
SERVER = 'host.docker.internal,1433'
DATABASE = 'Airflow'
username = 'airflow_user'
password = 'Test@12345!'


# ✅ ODBC connection string using Windows Authentication
CONN_STR = (
    f'DRIVER={{ODBC Driver 17 for SQL Server}};'
    f'SERVER={SERVER};'
    f'DATABASE={DATABASE};'
    f'UID={username};'
    f'PWD={password};'
    f'Encrypt=yes;'
    f'TrustServerCertificate=yes;'

)

# ✅ Python function to load data
def load_to_sqlserver():
    print("Reading CSV...")
    df = pd.read_csv(CSV_PATH)
    df = df.fillna(0)
    print(f"Loaded {len(df)} rows.")

    print("Connecting to SQL Server...")
    conn = pyodbc.connect(CONN_STR)
    cursor = conn.cursor()

    # Clear table
    cursor.execute("DELETE FROM flight_delays;")

    conn.commit()

    # Insert query
    insert_query = """
    INSERT INTO flight_delays (
        year, month, carrier, carrier_name, airport, airport_name,
        arr_flights, arr_del15, carrier_ct, weather_ct, nas_ct,
        security_ct, late_aircraft_ct, arr_cancelled, arr_diverted,
        arr_delay, carrier_delay, weather_delay, nas_delay,
        security_delay, late_aircraft_delay
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    for _, row in df.iterrows():
        values = (
            int(row['year']),
            int(row['month']),
            str(row['carrier']),
            str(row['carrier_name']),
            str(row['airport']),
            str(row['airport_name']),
            int(row['arr_flights']),
            int(row['arr_del15']),
            float(row['carrier_ct']),
            float(row['weather_ct']),
            float(row['nas_ct']),
            float(row['security_ct']),
            float(row['late_aircraft_ct']),
            int(row['arr_cancelled']),
            int(row['arr_diverted']),
            int(row['arr_delay']),
            int(row['carrier_delay']),
            int(row['weather_delay']),
            int(row['nas_delay']),
            int(row['security_delay']),
            int(row['late_aircraft_delay']),
        )
        cursor.execute(insert_query, values)

    conn.commit()
    cursor.close()
    conn.close()
    print("✅ Data successfully loaded to SQL Server.")

# ✅ Airflow DAG definition
default_args = {
    'start_date': datetime(2024, 1, 1),
    'retries': 0,
}

with DAG(
    dag_id='load_flight_data_to_sqlserver',
    default_args=default_args,
    schedule_interval=None,  # Manual trigger
    catchup=False,
    description='Loads cleaned flight delay data into SQL Server using pyodbc'
) as dag:

    load_task = PythonOperator(
        task_id='load_to_sqlserver_task',
        python_callable=load_to_sqlserver
    )
