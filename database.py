import time
import os
from google.cloud import bigquery
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'tokens/bq/kitrum-cloud.json'


class BigQuery:
    client = bigquery.Client()

    def insert_to_bigquery(self, messages_lists, table_id):
        for messages_list in messages_lists:
            job_config = bigquery.LoadJobConfig()
            job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
            job = self.client.load_table_from_json(messages_list, table_id, job_config=job_config)
            job.result()
            time.sleep(5)

    def get_from_bigquery(self, query):
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'kitrum-cloud-1d2f74eae0ed.json'
        query_job = self.client.query(query)
        print(f"Requesting Data from BigQuery according to query: {query}")
        bq_data = []
        for row in query_job.result():
            row_dict = {}
            for key in row.keys():
                row_dict[key] = row[key]
            bq_data.append(row_dict)
        return bq_data

