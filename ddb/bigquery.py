

import os
from google.cloud import bigquery


#%% BigQuery

class BigQuery:
    def __init__(self
                 , credentials_filepath = os.getenv('bigquery_cred')
                 ):

        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_filepath

        self.client = bigquery.Client()

    def read(self, sql):
        return self.client.query(sql).to_dataframe()