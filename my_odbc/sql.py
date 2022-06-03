
import os
import pyodbc
import urllib
from sqlalchemy import create_engine
import pandas as pd


#%% Microsoft SQL

class SQL:
    def __init__(self
                 , db = os.getenv('odbc_db')
                 , server = os.getenv('odbc_server')
                 , uid = os.getenv('odbc_uid')
                 , pwd = os.getenv('odbc_pwd')
                 , local_cred = 'yes'
                 , driver = os.getenv('odbc_driver')
                 ):
        
        if local_cred=='yes':
            uid_str = ''
            pwd_str = ''
            trusted_conn_str = 'trusted_connection=yes'
        else:
            uid_str = f'UID={uid};'
            pwd_str = f'PWD={pwd};'
            trusted_conn_str = ''
        driver_str = f'DRIVER={driver};'
        server_str = f'SERVER={server};'
        db_str = f'DATABASE={db};'

        self.con_str = driver_str + server_str + db_str + trusted_conn_str + uid_str + pwd_str
        con_str_write = urllib.parse.quote_plus(self.con_str)
        self.con = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(con_str_write), fast_executemany=True)

    def read(self, sql):
        return pd.read_sql_query(sql=sql, con=self.con)

    def run(self, sql, auto_commit=False):
        with pyodbc.connect(self.con_str) as con_pyodbc:
            if auto_commit==True:
                con_pyodbc.autocommit = True
            else:
                con_pyodbc.autocommit = False

            with con_pyodbc.cursor() as cursor:
                cursor.execute(sql)
                while cursor.nextset():
                    pass
                if auto_commit==False:
                    con_pyodbc.commit()


    def __update_dtype(self, df, column, dtype):
        dict_dtype = {
            'object':'varchar(max_len_a)',
            'int64':'int',
            'float64':'decimal(max_len_b, max_len_a)',
            'bool':'bit',
            'datetime64':'datetime',
            'datetime64[ns]':'datetime',
            }
        dtype = str(dtype)
        def float_size(x):
            if '.' in str(x):
                return len(str(x).split('.')[1])
            else:
                return 0
        max_len_a = ''
        max_len_b = ''
        if dtype == 'object':
            max_len_a = max(df[column].apply(lambda x: len(str(x)) if pd.notnull(x) else 0)) + 5
        elif dtype == 'float64':
            max_len_a = max(df[column].apply(lambda x: float_size(x) if pd.notnull(x) else 0))
            max_len_b = 15 + max_len_a
        sql_type = dict_dtype[str(dtype)]
        sql_type = sql_type.replace('max_len_a', str(max_len_a))
        sql_type = sql_type.replace('max_len_b', str(max_len_b))
        sql_column = f'[{column}] {sql_type}'
        return sql_column

    def create_table(self, df, name, schema, replace=False):
        column_list = []
        for column, dtype in df.dtypes.iteritems():
            if column != 'RowLoadDateTime':
                column_list.append(self.__update_dtype(df, column, dtype))
        columns = ',\n'.join(column_list)
        sql_create = ''
        if replace == True:
            sql_create += f"DROP TABLE IF EXISTS {schema}.{name}\n"
        sql_create += f"""CREATE TABLE {schema}.{name} (
        	ID{name} INT IDENTITY(1, 1) NOT NULL,
            {columns},
        	RowLoadDateTime DATETIME NOT NULL CONSTRAINT DF_{name}_RowLoad DEFAULT (GETDATE()),
        	CONSTRAINT PK_{name} PRIMARY KEY (ID{name})
        )"""
        self.run(sql_create)

    def where_not_exists(self, df, table, columns):
        columns = list(columns)
        columns_sql = []
        columns_df = df.columns
        for i in range(len(columns)):
            columns_sql.append('[' + columns[i] + ']')
        sql_primkeys = f"select {','.join(columns_sql)} from {table}"
        df_primkeys = self.read(sql_primkeys)
        for col in df_primkeys.columns:
            if df[col].dtype != df_primkeys[col].dtype:
                df_primkeys[col] = df_primkeys[col].astype(df[col].dtype)
        df = pd.merge(left=df, right=df_primkeys, how="outer", on=columns, indicator=True)
        df = df[df['_merge'] == "left_only"]
        df = df[columns_df].reset_index(drop=True)
        return df

    def add_missing_columns(self, df, table):
        sql_current = f"SELECT TOP 1 * FROM {table}"
        df_current = self.read(sql_current)
        columns_current = df_current.columns
        for column, dtype in df.dtypes.iteritems():
            if column not in columns_current:
                sql_new = self.__update_dtype(df, column, dtype)
                sql_new = f"ALTER TABLE {table} ADD {sql_new}"
                self.run(sql_new)
