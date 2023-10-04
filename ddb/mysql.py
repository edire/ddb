
import os
from sqlalchemy import create_engine, text
import pandas as pd
import numpy as np
from  urllib.parse import quote_plus


#%% MySQL

class SQL:
    def __init__(self
                 , db = os.getenv('MYSQL_DB')
                 , server = os.getenv('MYSQL_SERVER')
                 , uid = os.getenv('MYSQL_UID')
                 , pwd = os.getenv('MYSQL_PWD')
                 , port = 3306
                 ):

        try:
            self.con = create_engine(f'mysql+pymysql://{uid}:{pwd}@{server}:{port}/{db}', isolation_level="AUTOCOMMIT")
            self.run('SELECT 1')
        except:
            pwd = quote_plus(pwd)
            self.con = create_engine(f'mysql+pymysql://{uid}:{pwd}@{server}:{port}/{db}', isolation_level="AUTOCOMMIT")
            self.run('SELECT 1')

    def read(self, sql):
        sql = sql.replace('\ufeff', '')
        sql_list = sql.split(';')
        for ele in range(len(sql_list)):
            sql_list[ele] = sql_list[ele].strip()
            if len(sql_list[ele]) == 0:
                del(sql_list[ele])
        with self.con.connect() as connection:
            for ele in range(len(sql_list) - 1):
                connection.execute(text(sql_list[ele]))
            df = pd.read_sql_query(sql=sql_list[-1], con=connection)
        return df

    def run(self, sql):
        con_pymysql = self.con.raw_connection()
        with con_pymysql.cursor() as cursor:
            for query in sql.strip().split(';'):
                if len(query) > 0:
                    cursor.execute(query + ';')

    def __update_dtype(self, df, column, dtype):
        dict_dtype = {
            'object':'varchar(max_len_a)',
            'int64':'max_len_aint',
            'float64':'decimal(max_len_a, max_len_b)',
            'bool':'bit',
            'datetime64':'datetime',
            'datetime64[ns]':'datetime',
            }
        dtype = str(dtype)
        def float_size(x, front=True):
            spl = 0 if front==True else 1
            if '.' in str(x):
                ln = len(str(x).split('.')[spl])
            else:
                ln = len(str(x)) if front==True else 0
            return ln
        max_len_a = ''
        max_len_b = ''
        if dtype == 'object':
            max_len_a = max(df[column].apply(lambda x: len(str(x)) if pd.notnull(x) else 0)) + 5
        elif dtype == 'float64':
            max_len_a = max(df[column].apply(lambda x: float_size(x, front=True) if pd.notnull(x) else 0))
            max_len_b = max(df[column].apply(lambda x: float_size(x, front=False) if pd.notnull(x) else 0))
            max_len_a = max_len_a + max_len_b + 2
        elif dtype == 'int64':
            if df[column].abs().max() <= 99:
                max_len_a = 'tiny'
            elif df[column].abs().max() <= 9999:
                max_len_a = 'small'
            elif df[column].abs().max() > 999999999:
                max_len_a = 'big'
        sql_type = dict_dtype[str(dtype)]
        sql_type = sql_type.replace('max_len_a', str(max_len_a))
        sql_type = sql_type.replace('max_len_b', str(max_len_b))
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
            ID{name} INT AUTO_INCREMENT NOT NULL,
            {columns},
            RowLoadDateTime DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            RowModifiedDateTime DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (ID{name})
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
                sql_new = f"ALTER TABLE {name} ADD {sql_new};"
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
        df_copy.to_sql(name, if_exists='append', index=False, con=self.con)
        
        return True