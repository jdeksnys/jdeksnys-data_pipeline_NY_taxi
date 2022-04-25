from datetime import datetime
import os
os.system('Clear')
from configparser import ConfigParser
import psycopg2
import pandas as pd
import datetime



# configure database credentials, connect to db
conf_folder_path=os.path.abspath(__file__).replace("/Postgres/import_sql.py","")
file=conf_folder_path+"/config/config.ini"

config=ConfigParser()
config.read(file)
dbname=config.get('db_login','dbname')
user=config.get('db_login','user')
password=config.get('db_login','password')
host=config.get('db_login','host')

try:
    db_conn=psycopg2.connect(dbname=dbname,user=user,password=password,host=host)
    db_cur=db_conn.cursor()
    # log.info('OK db connection')
except:
    print('error')
    # log.fail('ERROR db connection failed')



# specify where dowloaded csv file are stored
folder_dir='/Users/jonasdeksnys/Desktop/taxi_data'
file_templates={'yellow_tripdata':{'table':'yellow_taxi_trip_records_csv','col':'tpep_pickup_datetime'},'green_tripdata':{'table':'green_taxi_trip_records_csv','col':'lpep_pickup_datetime'},'fhv_tripdata':{'table':'for_hire_vehicle_trip_records_csv','col':'pickup_datetime'},'fhvhv_tripdata':'pickup_datetime'}
csv_files=os.listdir(folder_dir)



# check if files for insert not laready inserted
for j in csv_files:
    for n in file_templates:
        if n in j:


            # get latest record in csv file
            file_dir=folder_dir + '/' + j
            print(file_dir)
            df=pd.read_csv('/Users/jonasdeksnys/Desktop/taxi_data/yellow_tripdata_2021-01.csv').sort_values(by=file_templates[n]['col'],ascending=False)
            df.loc[0,file_templates[n]['col']]
            latest_rec_csv=df.head(1)


            # get latest record in database
            query=f"SELECT {file_templates[n]['col']} FROM raw_data_schema.{file_templates[n]['table']} ORDER BY {file_templates[n]['col']} DESC LIMIT 1"
            print(query)
            db_cur.execute(query)
            latest_rec_db=(db_cur.fetchone())[0]


            # insert csv into database
            if datetime.date(latest_rec_csv)>datetime.date(latest_rec_db):
                df=pd.read_csv(file_templates[n],header=1)
                table=f"raw_data_schema.{file_templates[n]['table']}"
                print(table)
                with open(file_dir,'r') as f:
                    next(f)
                    db_cur.copy_from(f,table,sep=',')