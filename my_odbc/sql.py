
import os
import urllib
from sqlalchemy import create_engine
import pandas as pd


#%% Microsoft SQL

class SQL:
    def __init__(self
                 , db = os.getenv('odbc_db')
                 , server = os.getenv('odbc_server')
                 , local_cred = 'yes'
                 , uid = os.getenv('odbc_uid')
                 , pwd = os.getenv('odbc_pwd')
                 , driver = 'ODBC Driver 17 for SQL Server'
                 , fast_executemany = True
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

        con_str_write = urllib.parse.quote_plus(driver_str + server_str + db_str + trusted_conn_str + uid_str + pwd_str)
        self.con = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(con_str_write), fast_executemany=fast_executemany)

    def read(self, sql):
        return pd.read_sql_query(sql=sql, con=self.con)

    def run(self, sql, auto_commit=False):
        con_pyodbc = self.con.raw_connection()

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
            'int64':'max_len_aint',
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
        sql_column = f'[{column}] {sql_type}'
        return sql_column

    def create_table(self, df, name, schema='dbo', replace=False, extras=False):
        column_list = []
        for column, dtype in df.dtypes.iteritems():
            column_list.append(self.__update_dtype(df, column, dtype))
            if 'RowLoadDateTime' in column_list and extras == True:
                column_list.remove('RowLoadDateTime')
        columns = ',\n'.join(column_list)
        sql_create = ''
        if replace == True:
            sql_create += f"DROP TABLE IF EXISTS {schema}.{name}\n"
        sql_create += f"CREATE TABLE {schema}.{name} ("
        if extras == True:
            sql_create += f"""
            ID{name} INT IDENTITY(1, 1) NOT NULL,
            {columns},
            RowLoadDateTime DATETIME NOT NULL CONSTRAINT DF_{name}_RowLoad DEFAULT (GETDATE()),
            CONSTRAINT PK_{name} PRIMARY KEY (ID{name})
            )"""
        else:
            sql_create += f"{columns})"
        self.run(sql_create)

    def where_not_exists(self, df, name, schema, columns):
        columns = list(columns)
        columns_sql = []
        columns_df = df.columns
        for i in range(len(columns)):
            columns_sql.append('[' + columns[i] + ']')
        sql_primkeys = f"select {','.join(columns_sql)} from {schema}.{name}"
        df_primkeys = self.read(sql_primkeys)
        for col in df_primkeys.columns:
            if df[col].dtype != df_primkeys[col].dtype:
                df_primkeys[col] = df_primkeys[col].astype(df[col].dtype)
        df = pd.merge(left=df, right=df_primkeys, how="outer", on=columns, indicator=True)
        df = df[df['_merge'] == "left_only"]
        df = df[columns_df].reset_index(drop=True)
        return df

    def add_missing_columns(self, df, name, schema='dbo'):
        sql_current = f"SELECT TOP 1 * FROM {schema}.{name}"
        df_current = self.read(sql_current)
        columns_current = df_current.columns
        for column, dtype in df.dtypes.iteritems():
            if column not in columns_current:
                sql_new = self.__update_dtype(df, column, dtype)
                sql_new = f"ALTER TABLE {schema}.{name} ADD {sql_new}"
                self.run(sql_new)

    def to_sql(self, df, name, schema='dbo', if_exists='fail', index=True, **kwargs):
        df_copy = df.copy()
        if index == True:
            df_copy.reset_index(inplace=True)

        if if_exists == 'replace':
            self.create_table(df_copy, name, schema, replace=True, **kwargs)
        elif if_exists == 'fail':
            self.create_table(df_copy, name, schema, replace=False, **kwargs)
        elif if_exists == 'append':
            try:
                self.create_table(df_copy, name, schema, replace=False, **kwargs)
            except:
                pass
        else:
            raise(Exception('if_exists value is invalid, please choose between (fail, replace, append)'))

        sql = self.__get_insert_query(df_copy, name, schema)
        vals = list(df_copy.itertuples(index=False))
        self.con.execute(sql, vals)
        return True

    def __get_insert_query(self, df, name, schema):
        list_columns = [f'[{x}]' for x in df.columns]
        list_values = ['?'] * len(list_columns)

        sql_columns = ', '.join(list_columns)
        sql_values = ', '.join(list_values)
        sql = f'INSERT INTO {schema}.{name} ({sql_columns}) VALUES ({sql_values})'
        return sql
