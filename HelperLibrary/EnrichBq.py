import typing
import pyodbc
import pandas as pd
#from google.cloud import bigquery
from HelperLibrary.HdeUtilities import *
import configparser

class EnrichBq:
    def __init__(self) -> None:
        self.schema=[
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
        self.schema_datatypes=[
                bigquery.SchemaField("TlmId", "INTEGER"),
                bigquery.SchemaField("Facility", "STRING"),
                bigquery.SchemaField("AppReconcile", "STRING"),
                bigquery.SchemaField("MsgEvent", "STRING"),
                bigquery.SchemaField("MsgType", "STRING"),
                bigquery.SchemaField("UMID", "STRING"),
                bigquery.SchemaField("MsgControlId", "STRING"),
                bigquery.SchemaField("MsgDatetime", "TIMESTAMP"),
                bigquery.SchemaField("App", "STRING"),
                bigquery.SchemaField("ReceiveDatetime", "TIMESTAMP"),
                bigquery.SchemaField("ActivityDatetime", "TIMESTAMP"),
            ]

        # https://stackoverflow.com/questions/42906665/import-my-database-connection-with-python
        # pip install configparser
        # pip3 install configparser
        config = configparser.ConfigParser()
        config.read('config.ini')
        self.TLM_DB_Server = config['mssqlDB']['host']
        self.TLM_user = config['mssqlDB']['user']
        self.TLM_dev_pass = config['mssqlDB']['pass']
        self.TLM_DB = config['mssqlDB']['db']
    
        self.project_id = config['gcp']['project_id']
        self.table_id = self.project_id + ".MCID_comparison.tlmmcid"

        self.dbServer = self.TLM_DB_Server # os.environ.get("TLM_DB_Server") # 
        self.db = self.TLM_DB # os.environ.get("TLM_DB") # 
        self.username = self.TLM_user # os.environ.get("TLM_User") # 
        self.password = self.TLM_dev_pass # os.environ.get("TLM_dev_pass") # 
        self.initDbConn = 'DRIVER={ODBC Driver 18 for SQL Server};SERVER='+self.dbServer+';DATABASE='+self.db+';UID='+self.username+';PWD='+self.password+';TrustServerCertificate=yes;'

    def load_bq_from_csv(self):
        # csv_McidFile = '../../GcpHde/Mcid/TlmMcidAll20230927.csv'
        csv_McidFile = 'TlmMcidVxuTest20231003.csv'
        # project_id = self.project_id
        table_id ='MCID_comparison.tlmmcid'

        ## run below code to load CSV files
        gcp_bq_df_to_bq_table(self.project_id,table_id,csv_McidFile)

    def load_bq_from_local_file(self):
        '''
        # 20231006 - working - finished testing and done
        #
        Reference: https://cloud.google.com/bigquery/docs/samples/bigquery-load-from-file?hl=en
        '''
        from google.cloud import bigquery

        # Construct a BigQuery client object.
        client = bigquery.Client()

        # TODO(developer): Set table_id to the ID of the table to create.
        # table_id = "hcahde040-stage-data.MCID_comparison.tlmmcid"
        # table_id = client.dataset('my_dataset').table('existing_table')

        msgType=["VXU","ADT","ORU","MDM","ORM","RAS","RDE","PPR","SIU"]
        for mType in msgType:
            print("============================================================================================ Msg Type"+mType)
            file_path = './TlmMcid'+mType+'Test20231003.csv'
            print('Before '+file_path)
            # job_config = bigquery.LoadJobConfig(
            #     source_format=bigquery.SourceFormat.CSV,
            #     skip_leading_rows=0,schema=schema,
            #     autodetect=True
            # )
            job_config = bigquery.LoadJobConfig()
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
            job_config.skip_leading_rows = 0
            job_config.schema=self.schema
            job_config.autodetect=True

            # Source format defaults to CSV, so the line below is optional.
            job_config.source_format = bigquery.SourceFormat.CSV

            with open(file_path, "rb") as source_file:
                job = client.load_table_from_file(source_file, self.table_id, job_config=job_config)

            job.result()  # Waits for the job to complete.

            table = client.get_table(table_id)  # Make an API request.
            print(
                "Loaded {} rows and {} columns to {}".format(
                    table.num_rows, len(table.schema), table_id
                )
            )

    def pandas_sql_csv(self):
        '''
        # 20231006 - working - finished testing and done
        '''
        # for a named instance, use # server = 'localhost\sqlexpress'
        # to specify an alternate port for server, use # server = 'myserver,port'

        # dbConn = pyodbc.connect('DRIVER={SQL Server};SERVER='+dbServer+';DATABASE='+db+';UID='+username+';PWD='+ password)
        # dbConn = pyodbc.connect('DRIVER={SQL Server Native Client 18.0};SERVER='+dbServer+';DATABASE='+db+';UID='+username+';PWD='+password+';Trusted_Connection=yes;')
        dbConn = pyodbc.connect(self.initDbConn)
        # select or SPROC to fetch records
        sql = "EXEC [dbo].[usp_GetHdeGcpTest] ?,?,?"
        cursor = dbConn.cursor()
        # 20231003
        maxId=138254751134
        minId=138200773269
        msgDt='20231003'
        # 20231003
        maxId=138307755064
        minId=138254751135
        msgDt='20231004'
        msgType=["VXU","ADT","ORU","MDM","ORM","RAS","RDE","PPR","SIU"]
        msgType=["VXU"]
        # params=("MsgType":"VXU","MinId":minId,"MaxId":maxId)

        #rows = cursor.execute(sql,params).fetchall()
        # cursor.close()

        for mType in msgType:
            print("============================================================================================ Msg Type"+mType)
            params=(mType,minId,maxId)
            df = pd.read_sql_query(sql=sql,con= dbConn,params=params,index_col=None,coerce_float=True)
            df.to_csv("TlmMcid"+mType+"Test"+msgDt+".csv",index=False,header=False)
            print(df.head(5))



    def pandas_sql_df_to_bq(self):
        '''
        # 20231006 - testing
        '''
        # for a named instance, use # server = 'localhost\sqlexpress'
        # to specify an alternate port for server, use # server = 'myserver,port'
        dbConn = pyodbc.connect(self.initDbConn)
        # select or SPROC to fetch records
        sql = "EXEC [dbo].[usp_GetHdeGcpTest] ?,?,?"
        # cursor = dbConn.cursor()
        # 20231003
        maxId=138254751134
        minId=138200773269
        msgDt='20231003'
        # 20231003
        maxId=138307755064
        minId=138254751135
        msgDt='20231004'
        msgType=["VXU","ADT","ORU","MDM","ORM","RAS","RDE","PPR","SIU"]
        # msgType=["VXU"]
        # params=("MsgType":"VXU","MinId":minId,"MaxId":maxId)

        #rows = cursor.execute(sql,params).fetchall()
        # cursor.close()

        # project_id = self.project_id
        # table_id ='MCID_comparison.tlmmcid'

        for mType in msgType:
            print("============================================================================================ Msg Type"+mType)
            params=(mType,minId,maxId)
            df = pd.read_sql_query(sql=sql,con=dbConn,params=params,index_col=None,coerce_float=True)
            # df.to_csv("TlmMcid"+mType+"Test"+msgDt+".csv",index=False,header=False)
            print(df.head(5))
            pandas_gbq.to_gbq(df, self.table_id, project_id=self.project_id)
            print('DONE in pandas_sql_df_to_bq')


    # print(select_driver())  # ODBC Driver 17 for SQL Server
    def select_driver():
        """Find least version of: ODBC Driver for SQL Server."""
        drv = sorted([drv for drv in pyodbc.drivers() if "ODBC Driver " in drv and " for SQL Server" in drv])
        if len(drv) == 0:
            raise Exception("No 'ODBC Driver XX for SQL Server' found.")
        return drv[-1]


    # load_table_dataframe(table_id: str) -> "bigquery.Table":
    def load_table_dataframe(self,pMinId,pMaxId,pMsgDt) -> "bigquery.Table":
        '''
        # Reference: https://cloud.google.com/bigquery/docs/samples/bigquery-load-table-dataframe?hl=en
        '''
        '''
        # 20231006 - working - finished testing and done
        '''
        dbConn = pyodbc.connect(self.initDbConn)
        # select or SPROC to fetch records
        sql = "EXEC [dbo].[usp_GetHdeGcpTest] ?,?,?"
        # cursor = dbConn.cursor()
        # 20231003
        # maxId=138254751134
        # minId=138200773269
        # msgDt='20231003'
        # # 20231003
        # maxId=138307755064
        # minId=138254751135
        # msgDt='20231004'
        # Assign params from input arguments
        print("minId {} maxId {} msgDt {}".format(pMinId,pMaxId,pMsgDt))
        maxId=pMaxId
        minId=pMinId
        msgDt=pMsgDt
        # msgType=["VXU","ADT","ORU","MDM","ORM","RAS","RDE","PPR","SIU"]
        msgType=["VXU","ADT","ORU","MDM","ORM","RAS","RDE","PPR","SIU"]
        # msgType=["VXU"]
        # params=("MsgType":"VXU","MinId":minId,"MaxId":maxId)

        #rows = cursor.execute(sql,params).fetchall()
        # cursor.close()

        # [START bigquery_load_table_dataframe]
        # import datetime

        # from google.cloud import bigquery
        # import pandas
        # import pytz

        # Construct a BigQuery client object.
        client = bigquery.Client()

        # TODO(developer): Set table_id to the ID of the table to create.
        # table_id = "your-project.your_dataset.your_table_name"
        # table_id = self.project_id + ".MCID_comparison.tlmmcid"

        job_config = bigquery.LoadJobConfig()
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
        job_config.skip_leading_rows = 0
        job_config.schema=self.schema
        job_config.autodetect=True

        # Source format defaults to CSV, so the line below is optional.
        job_config.source_format = bigquery.SourceFormat.CSV

        for mType in msgType:
            print("============================================================================================ Msg Type"+mType)
            params=(mType,minId,maxId)
            df = pd.read_sql_query(sql=sql,con=dbConn,params=params,index_col=None,coerce_float=True)
            print(df.head(5))
            print(df.count())
            job = client.load_table_from_dataframe(
                df, self.table_id, job_config=job_config
            )  # Make an API request.
            job.result()  # Wait for the job to complete.

            ## Uncomment below for getting BigQuery table counts for each iteration
            # table = client.get_table(self.table_id)  # Make an API request.
            # print(
            #     "Loaded {} rows and {} columns to {}".format(
            #         table.num_rows, len(table.schema), self.table_id
            #     )
            # )
            
            # [END bigquery_load_table_dataframe]
        # return table


    def pre_process_sql_init(self):
        sql="select top 100 * from tlm_rpt_link.tlm_rpt.dbo.tlmdailyvolume with(nolock) \
                where tablename='TLM_RECONCILIATION_UMID' and RowsCountDate>'2023-09-29' \
            order by id"
        dbConn = pyodbc.connect(self.initDbConn)
        df = pd.read_sql_query(sql=sql,con=dbConn,index_col=None,coerce_float=True)
        for row in df.itertuples(index=False):
            print(row)
            minId=row[7]
            maxId=row[6]
            msgDt=row[2]
            print("minId {} maxId {} msgDt {}".format(minId,maxId,msgDt))
            self.load_table_dataframe(minId,maxId,msgDt)
            print("======================================================================================================== Starting With Next Row Of Records")
