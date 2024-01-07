--------------------------------------------------------------------------------------------------
-- Common Functions
--------------------------------------------------------------------------------------------------
TIMESTAMP_TRUNC(CAST(msg_date_nested.value.instant AS TIMESTAMP), DAY) IN ("2023-09-29")

SPLIT(mcid_value.value.string, "_")[1] IN (SELECT TRIM(Mnemonic) FROM hcahde040-stage-data.day2ops_metrics_alerts.FacilityList)

--------------------------------------------------------------------------------------------------
-- MCID
--------------------------------------------------------------------------------------------------
WITH cloverleaf_mcid AS (
    SELECT * 
    FROM `hcahde040-stage-data.MCID_comparison.hca_mcid_results_partitioned` 
    WHERE transaction_date = "2023-09-29"
    ),
final_fhir_mcid AS (
    SELECT DISTINCT id,msg_date_nested.value.instant AS msh_7, mcid_value.value.string AS final_fhir_mcid 
    FROM hcahde040-stage-data.final_fhir.Provenance, unnest(MessageControlId) as MCID, unnest(MCID.Value) as mcid_value
        ,unnest(MessageDateTime) as msg_date,unnest(msg_date.value) as msg_date_nested 
    WHERE TIMESTAMP_TRUNC(CAST(msg_date_nested.value.instant AS TIMESTAMP), DAY) IN ("2023-09-29")
        AND SPLIT(mcid_value.value.string, "_")[1] IN (SELECT TRIM(Mnemonic) FROM hcahde040-stage-data.day2ops_metrics_alerts.FacilityList)
    )
SELECT * 
from final_fhir_mcid 
LEFT JOIN cloverleaf_mcid ON cloverleaf_mcid.TLM_UMID = final_fhir_mcid.final_fhir_mcid
WHERE cloverleaf_mcid.TLM_UMID IS NULL


--------------------------------------------------------------------------------------------------
-- INFORMATION_SCHEMA
--------------------------------------------------------------------------------------------------

--Reference: https://cloud.google.com/bigquery/docs/information-schema-jobs#calculate_average_slot_utilization
SELECT
 job_id,
 creation_time,
 query
FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_USER
WHERE state != "DONE"


--Reference: https://cloud.google.com/bigquery/docs/information-schema-jobs
--https://cloud.google.com/bigquery/docs/information-schema-jobs#load-job-quota
--Get the number of load jobs to determine the daily job quota used
SELECT
    DATE(creation_time) as day,
    destination_table.project_id as project_id,
    destination_table.dataset_id as dataset_id,
    destination_table.table_id as table_id,
    COUNT(job_id) AS load_job_count
 FROM
   region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT
 WHERE
    creation_time BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 8 DAY) AND CURRENT_TIMESTAMP()
    AND job_type = "LOAD"
GROUP BY
    day,
    project_id,
    dataset_id,
    table_id
ORDER BY
    day DESC



--Reference: https://cloud.google.com/architecture/monitoring-metric-export#sample_query_bigquery_query_counts
--The following query returns the number of queries against BigQuery per day in a project.
SELECT
  EXTRACT(DATE  FROM point.interval.end_time) AS extract_date,
  sum(point.value.int64_value) as query_cnt
FROM
  `sage-facet-201016.metric_export.sd_metrics_export`
CROSS JOIN
  UNNEST(resource.labels) AS resource_labels
WHERE
   point.interval.end_time > TIMESTAMP(DATE_SUB(CURRENT_DATE, INTERVAL 30 DAY))
  AND  point.interval.end_time <= CURRENT_TIMESTAMP
  and metric.type = 'bigquery.googleapis.com/query/count'
  AND resource_labels.key = "project_id"
  AND resource_labels.value = "sage-facet-201016"
group by extract_date
order by extract_date

