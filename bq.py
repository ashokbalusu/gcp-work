from google.cloud import bigquery

client = bigquery.Client()

# TODO(dev): Change table_id to the full name of the table you want to create.
table_id ="hcahde040-stage-data.MCID_comparison.TlmMcidAll20230927"

table = client.get_table(table_id)  # API Request

# View table labels
print(f"Table ID: {table_id}.")
if table.labels:
    for label, value in table.labels.items():
        print(f"\t{label}: {value}")
else:
    print("\tTable has no labels defined.")

