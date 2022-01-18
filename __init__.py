import datetime
import logging
import json
import psycopg2
import requests
import azure.functions as func
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
import os




def main(mytimer: func.TimerRequest) -> None:

    # get api request
    try:
        r=requests.get("https://data.cityofnewyork.us/resource/hvrh-b6nb.json?$$app_token={token}")
        if r.status_code==200:
            r=r.json()
            r=json.dumps(r)
            logging.info("OK api called, response ok (200)")    
        else:
            logging.error("ERROR api response fail")
            raise Exception(r.status_code)
    except Exception:
        pass
        logging.fail("ERROR api call fail. No data loaded")



    # configure database credentials as key-vault secrets. keyVaultName=os.environ["KEY_VAULT_NAME"]
    KVUri=f"https://{KEY_VAULT_NAME}}.vault.azure.net"
    credential=DefaultAzureCredential()
    client=SecretClient(vault_url=KVUri,credential=credential)

    dbname=client.get_secret("dbname").value
    user=client.get_secret("user").value
    password=client.get_secret("password").value
    host=client.get_secret("host").value



    # connect to database
    try:
        db_conn=psycopg2.connect(dbname=dbname,user=user,password=password,host=host)
        db_cur=db_conn.cursor()
        logging.info('OK db connection')
    except:
        logging.fail('ERROR db connection failed')
        raise Exception('error')



    # parse and insert json from api response into database
    query=f"""
    INSERT INTO raw_data_schema.green_taxi_api_tabular
        (
            SELECT * FROM json_populate_recordset
                (
                    NULL::raw_data_schema.green_taxi_api_tabular,
                    \'{r}\'
                )
        );
    """

    try:
        db_cur.execute(query)
        db_conn.commit()
        logging.info('OK api json parsed&inserted to db.')
    except:
        logging.error('ERROR json not parsed/inserted into db.')
        raise Exception

    db_cur.close()
    db_conn.close()