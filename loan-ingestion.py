from datetime import datetime, timedelta

from airflow import DAG, macros
from airflow.operators.python import PythonOperator
from airflow.contrib.sensors.file_sensor import FileSensor

default_args = {
    'owner': 'muqtafi',
    'depends_on_past': False,
    'email': ['muqtafiakhmad@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=10)
}

with DAG(
    'loan-ingestion',
    default_args=default_args,
    description='daily loan data ingestion',
    schedule_interval=timedelta(days=1),
    # set September 2nd as start date
    start_date=datetime(2021,9,2),
    tags=['daily', 'credit', 'loan'],
) as dag:
    # construct folder path
    date_str = "{{ (execution_date-macros.timedelta(days=1)).strftime('%Y/%m/%d') }}"
    folder_path = '/Users/muqtafiakhmad/Desktop/credit/credit-data-ingestion/sample/'+date_str

    file_sensor = FileSensor(
        task_id='detect-folder', 
        poke_interval=30, 
        filepath=folder_path)
    
    def upload_csv(folder_path, insert_date):
        # get file list
        from os import listdir
        from os.path import isfile, join
        
        print('Listing files from {} '.format(folder_path))
        file_list = [join(folder_path, f) for f in listdir(folder_path) if isfile(join(folder_path, f))]

        # construct pd from file list
        import pandas as pd

        df_list = list(map(lambda file_name: pd.read_csv(file_name), file_list))

        df = pd.concat(df_list)

        # rename columns
        df.rename(columns={
            # there is an unnamed col, assign as "No"
            'Unnamed: 0':'No',
            # change column naming to title case
            'age':'Age'
        }, inplace=True)

        # convert number of dependents to int
        df['NumberOfDependents'] = df['NumberOfDependents'].astype('int64', errors='ignore')
        
        #add insert date
        df['InsertDate'] = pd.to_datetime(insert_date)

        # rename columns since mssql does not allow dash in column name
        df.rename(columns={
            'NumberOfTime30-59DaysPastDueNotWorse':'NumberOfTime30To59DaysPastDueNotWorse',
            'NumberOfTime60-89DaysPastDueNotWorse':'NumberOfTime60To89DaysPastDueNotWorse'
        }, inplace=True)

        # construct connection to mysql
        connection_string = 'mysql+mysqlconnector://{user}:{password}@{host}/{dbname}'.format(
            user='root',
            password='root',
            host='localhost',
            dbname='credit'
        )

        from sqlalchemy import create_engine

        engine = create_engine(connection_string)

        # delete previously inserted data (if any)
        print('Delete existing data for insert date {}'.format(insert_date))
        connection = engine.connect()
        connection.execute("""
            DELETE FROM credit.loan_application WHERE InsertDate = '{}'
        """.format(insert_date))

        # try to insert data
        print('Insert data for insert date {}'.format(insert_date))
        df.to_sql('loan_application', engine, if_exists='append', index=False)

    upload_csv_operator = PythonOperator(
        task_id='upload-csv',
        python_callable=upload_csv,
        op_kwargs={
            'folder_path': '/Users/muqtafiakhmad/Desktop/credit/credit-data-ingestion/sample/'+date_str,
            'insert_date': "{{ (execution_date-macros.timedelta(days=1)).strftime('%Y/%m/%d').replace('/', '-') }}"
        },
    )
    
    # define DAG
    file_sensor >> upload_csv_operator