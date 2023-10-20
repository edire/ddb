
import numpy as np
from datetime import datetime as dt


#%% Functions

def clean(df, rowloadtime=False, drop_cols=True):
    df.dropna(how='all', axis=0, inplace=True)
    if drop_cols == True:
        df.dropna(how='all', axis=1, inplace=True)
    df = clean_column_names(df)
    df = clean_data(df)
    if rowloadtime == True:
        df['RowLoadDateTime'] = dt.now()
    return df


def clean_data(df):
    df = df.map(__scrub_data)
    return df


def clean_column_names(df):
    lst_output = []
    for el in df.columns:
        lst_output.append(clean_string(el))
    df.columns = lst_output
    return df


def clean_string(str_input):
    str_input = str(str_input)
    for sc in [' ', '\\n']:
        str_input = str_input.replace(sc, '_')
        
    str_new = ''
    for ch in str_input:
        if ((ch.lower()>='a' and ch.lower()<='z') or (ch>='0' and ch<='9') or ch=='_'):
            str_new += ch

    while '__' in str_new:
        str_new = str_new.replace('__', '_')
    if len(str_new) > 1:
        if str_new[0] == '_':
            str_new = str_new[1:]
        if str_new[-1] == '_':
            str_new = str_new[:-1]

    return str_new


#%% Internal Functions

def __scrub_data(x):
    if isinstance(x, str):
        x = x.strip()
        if x == '':
            x = np.NaN
    elif isinstance(x, int) or isinstance(x, float):
        if x == 0:
            x = np.NaN
    return x