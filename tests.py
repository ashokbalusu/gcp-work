from HelperLibrary.HdeUtilities import *
from HelperLibrary.EnrichBq import *

ebq = EnrichBq()
print("starting...")
#print(ebq.select_driver())  # ODBC Driver 17 for SQL Server
## Use below to create local CSV files for TLM MCIDs
#ebq.pandas_sql_df()
### Create a BigQuery table for TLM MCIDs ## run TlmMcidLoadBq.py file
## below is not working. Throwing some errors
# ebq.load_bq_from_csv()

## Use below function to populate BigQuery with local CSV files with TLM MCIDs
# ebq.load_bq_from_local_file()

### below is not working. Throwing data type errors
# ebq.pandas_sql_df_to_bq()

## below is working to load directly from sql to BQ table using PD dataframes
# ebq.load_table_dataframe()

ebq.pre_process_sql_init()

print("DONE")
