

import os
from google.cloud import bigquery
import pandas as pd
import numpy as np


#%% BigQuery

class SQL:
    def __init__(self
                 , credentials_filepath = os.getenv('BIGQUERY_CRED')
                 ):

        if credentials_filepath != None:
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_filepath

        self.client = bigquery.Client()

    def read(self, sql):
        return self.client.query(sql).to_dataframe()
    
    def run(self, sql):
        for query in sql.strip().split(';'):
            if len(query) > 0:
                query_job = self.client.query(query)
                query_job.result()

    def __update_dtype(self, df, column, dtype):
        dict_dtype = {
            'object':'STRING',
            'int32':'INT64',
            'int64':'INT64',
            'float64':'FLOAT64',
            'bool':'BOOL',
            'datetime64':'DATETIME',
            'datetime64[ns]':'DATETIME',
            }
        dtype = str(dtype).lower()
        sql_type = dict_dtype[dtype]
        sql_column = f'`{column}` {sql_type}'
        return sql_column

    def create_table(self, df, name, replace=False, extras=False, **kwargs):
        column_list = []
        for column, dtype in df.dtypes.items():
            if column == 'RowLoadDateTime' and extras == True:
                pass
            else:
                column_list.append(self.__update_dtype(df, column, dtype))
        columns = ',\n'.join(column_list)
        sql_create = ''
        if replace == True:
            sql_create += f"DROP TABLE IF EXISTS {name};\n"
        sql_create += f"CREATE TABLE {name} ("
        if extras == True:
            sql_create += f"""
            {columns},
            RowLoadDateTime DATETIME NOT NULL
            );"""
        else:
            sql_create += f"{columns});"
        self.run(sql_create)

    def where_not_exists(self, df, name, columns):
        columns = list(columns)
        columns_sql = []
        columns_df = df.columns
        for i in range(len(columns)):
            columns_sql.append('`' + columns[i] + '`')
        sql_primkeys = f"select {','.join(columns_sql)} from {name};"
        df_primkeys = self.read(sql_primkeys)
        for col in df_primkeys.columns:
            if df[col].dtype != df_primkeys[col].dtype:
                df_primkeys[col] = df_primkeys[col].astype(df[col].dtype)
        df = pd.merge(left=df, right=df_primkeys, how="outer", on=columns, indicator=True)
        df = df[df['_merge'] == "left_only"]
        df = df[columns_df].reset_index(drop=True)
        return df

    def add_missing_columns(self, df, name):
        sql_current = f"SELECT * FROM {name} LIMIT 1;"
        df_current = self.read(sql_current)
        columns_current = df_current.columns
        for column, dtype in df.dtypes.items():
            if column not in columns_current:
                sql_new = self.__update_dtype(df, column, dtype)
                sql_new = f"ALTER TABLE {name} ADD COLUMN {sql_new};"
                self.run(sql_new)

    def to_sql(self, df, name, if_exists='fail', index=True, **kwargs):
        df_copy = df.copy()
        if index == True:
            df_copy.reset_index(inplace=True)

        if if_exists == 'replace':
            self.create_table(df_copy, name, replace=True, **kwargs)
        elif if_exists == 'fail':
            self.create_table(df_copy, name, replace=False, **kwargs)
        elif if_exists == 'append':
            try:
                self.create_table(df_copy, name, replace=False, **kwargs)
            except:
                pass
        else:
            raise(Exception('if_exists value is invalid, please choose between (fail, replace, append)'))

        df_copy = df_copy.replace({np.nan: None})

        job = self.client.load_table_from_dataframe(df_copy, name)
        job.result()

        return True