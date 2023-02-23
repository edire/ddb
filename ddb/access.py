
import pyodbc
import pandas as pd



#%% Microsoft Access

class SQL:
    def __init__(self,
                 filepath,
                 driver='DRIVER=Microsoft Access Driver (*.mdb, *.accdb);',
                 ):
        
        dbq= f'DBQ={filepath};'

        self.con_str = driver + dbq
        self.con = pyodbc.connect(self.con_str)

    def read(self, sql):
        return pd.read_sql_query(sql=sql, con=self.con)