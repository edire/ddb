
import pandas as pd
import sqlite3


#%% SQLite3

class SQL:
    def __init__(self, db):
        self.con = sqlite3.connect(db)
    
    def read(self, sql):
        return pd.read_sql_query(sql=sql, con=self.con)
    
    def run(self, sql):
        cursor = self.con.cursor()
        cursor.executescript(sql)
        self.con.commit()
            