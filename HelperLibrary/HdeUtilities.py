from google.cloud import bigquery
import pandas as pd
import pandas_gbq
from google.cloud.exceptions import NotFound
from datetime import datetime
import os

def gcp_bq_df_to_bq_table(project_id,table_id,csv_McidFile):
    '''
    uri = "gs://tlm-mcid/TlmMcidAll20230927.csv"
    csv_McidFile = '../../GcpHde/Mcid/TlmMcidAll20230927.csv'
    project_id ="hcahde040-stage-data"
    table_id ="MCID_comparison.TlmMcidAll20230927"
    gcp_bq_df_to_bq_table(uri,project_id,table_id,csv_McidFile)
    '''

    print('Before '+os.getcwd()+'/'+csv_McidFile)
    # mcidFile=os.getcwd()+'/'+csv_McidFile
    # df = pd.read_csv(csv_McidFile)
    df = pd.read_csv(csv_McidFile
                  ,index_col=0,header=0
                  ,sep=',',low_memory=False)
    print(df.head(5))
    pandas_gbq.to_gbq(df, table_id, project_id=project_id)
    print('DONE ', datetime.now())




# import csv 
# import logging
# import os
# # import cloudstorage as gcs
# import gcloud
# from gcloud import storage
# from google.cloud import bigquery
# from googleapiclient import discovery
# from oauth2client.client import GoogleCredentials
# import json
# import mysql.connector

# connecting to the DB 
# cnx = mysql.connector.connect(user="user", password="pass", host="11.111.111.11", database="test")
# cursor = cnx.cursor()
# SQLview = 'select * from test'
# filename = 'test_google2.csv'
# folder = "folder_path_to_file"

# # Creating CVS file
# cursor.execute(SQLview)
# with open(filename, 'w', newline= '') as f:
#     writer = csv.writer(f, delimiter=';')
#     writer.writerow([ i[0] for i in cursor.description ])
#     writer.writerows(cursor.fetchall())    

# uploading it into a bucket
def upload_blob(bucket_name, source_file_name, destination_blob_name):
    # upload_blob("bucket_name", folder + filename, filename)
    storage_client = storage.Client(project="project_name")
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)
    print('File {} uploaded to {}'.format(
        source_file_name,
        destination_blob_name
    ))

# inserting the csv from Cloud Storage into BigQuery
def insert_bigquery(target_uri, dataset_id, table_id):
    # insert_bigquery("gs://bucket_name/"+filename, "dataset_id", "table_id")
    bigquery_client = bigquery.Client(project="project_name")
    dataset_ref = bigquery_client.dataset(dataset_id)
    job_config = bigquery.LoadJobConfig()
    job_config.autodetect = True
    job_config.skip_leading_rows = 1
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.field_delimiter = ";"
    uri = target_uri
    load_job = bigquery_client.load_table_from_uri(
        uri,
        dataset_ref.table(table_id),
        job_config=job_config
        )
    print('Starting job {}'.format(load_job.job_id))
    load_job.result()
    print('Job finished.')


