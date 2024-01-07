from HelperLibrary.HdeUtilities import *

uri = 'gs://tlm-mcid/TlmMcidAll20230927.csv'
# csv_McidFile = '../../GcpHde/Mcid/TlmMcidAll20230927.csv'
csv_McidFile = 'TlmMcidVxuTest20231003.csv'
project_id ='hcahde040-stage-data'
table_id ='MCID_comparison.tlmmcid'

## run below code to load CSV files
# gcp_bq_df_to_bq_table(project_id,table_id,csv_McidFile)

# BigQuery client object.
client = bigquery.Client()

schema=[
        bigquery.SchemaField("TlmId", "STRING"),
        bigquery.SchemaField("Facility", "STRING"),
        bigquery.SchemaField("AppReconcile", "STRING"),
        bigquery.SchemaField("MsgEvent", "STRING"),
        bigquery.SchemaField("MsgType", "STRING"),
        bigquery.SchemaField("UMID", "STRING"),
        bigquery.SchemaField("MsgControlId", "STRING"),
        bigquery.SchemaField("MsgDatetime", "STRING"),
        bigquery.SchemaField("App", "STRING"),
        bigquery.SchemaField("ReceiveDatetime", "STRING"),
        bigquery.SchemaField("ActivityDatetime", "STRING"),
    ]

job_config = bigquery.LoadJobConfig(
    schema=schema,skip_leading_rows=0,
    autodetect=True,field_delimiter=',',
    # The source format defaults to CSV, so the line below is optional.
    source_format=bigquery.SourceFormat.CSV,
)
uri = "gs://tlm-mcid/TlmMcidAll20230927.csv"

# table_id ="hcahde040-stage-data.MCID_comparison.hca_cloverleaf_srcaud"
# table_id ="hcahde040-stage-data.MCID_comparison.TlmMcidAll20230927"
table_id=project_id + '.' + table_id

try:
    client.get_table(table_id)  # Make an API request.
    print("Table {} already exists.".format(table_id))

    # client.delete_table(table_id, not_found_ok=True)  # Make an API request.
    # print("Deleted table '{}'.".format(table_id))

except NotFound:
    print("Table {} is not found.".format(table_id))
    table = bigquery.Table(table_id,schema=schema)
    table = client.create_table(table)  # Make an API request
    print("Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))
    # print("********************* Table {} is created.".format(table_id))

# load_job = client.load_table_from_uri(
#     uri, table_id, job_config=job_config
# )  # Make an API request.

#load_job.result()  # Waits for the job to complete.

destination_table = client.get_table(table_id)  # Make an API request.
print("Loaded {} rows.".format(destination_table.num_rows))