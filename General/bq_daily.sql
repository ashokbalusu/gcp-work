--Reference: https://601aeef4538b8405-dot-us-east4.notebooks.googleusercontent.com/lab/tree/ingestion_team/emucollari/error_gathering_BQ.ipynb
--Error Pull
SELECT DISTINCT
file_path,
error_type,
file_path_timestamp,
MCID AS MCID_string,
SPLIT(file_path, "/")[SAFE_OFFSET(0)] AS pipeline,
FROM `hcahde040-stage-data.Stage_Dataflow_Error_Logging.errors_batch` 
WHERE TIMESTAMP(file_path_timestamp) > TIMESTAMP_SUB(TIMESTAMP(CURRENT_TIMESTAMP()), INTERVAL 7 DAY) AND file_path_timestamp != 'nan'
ORDER BY file_path_timestamp

--Materialized Views Information Schemas
SELECT table_name FROM hcahde040-stage-data.clincal_materialized_views.INFORMATION_SCHEMA.TABLES

