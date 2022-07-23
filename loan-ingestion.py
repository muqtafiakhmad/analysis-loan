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

        df_list = list(map(lambda file_name: pd.read_csv(
            file_name, 
            dtype = {
                'No':'Int32',
                'SeriousDlqin2yrs':'Int32',
                'RevolvingUtilizationOfUnsecuredLines': 'Float32',
                'Age':'Int32',
                'age':'Int32',
                'NumberOfTime30To59DaysPastDueNotWorse':'Int32',
                'NumberOfTime30-59DaysPastDueNotWorse':'Int32',
                'DebtRatio':'Float32',
                'MonthlyIncome':'Float64',
                'NumberOfOpenCreditLinesAndLoans':'Int32',
                'NumberOfTimes90DaysLate':'Int32',
                'NumberRealEstateLoansOrLines':'Int32',
                'NumberOfTime60To89DaysPastDueNotWorse':'Int32',
                'NumberOfTime60-89DaysPastDueNotWorse':'Int32',
                'NumberOfDependents':'Int32'
            }
        ), file_list))

        df = pd.concat(df_list)

        # rename columns
        df.rename(columns={
            'Unnamed: 0':'No',
            'age':'Age',
            'NumberOfTime30-59DaysPastDueNotWorse':'NumberOfTime30To59DaysPastDueNotWorse',
            'NumberOfTime60-89DaysPastDueNotWorse':'NumberOfTime60To89DaysPastDueNotWorse'
        }, inplace=True)
        
        #add insert date
        df['InsertDate'] = pd.to_datetime(insert_date)

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