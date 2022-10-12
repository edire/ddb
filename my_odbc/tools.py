
import numpy as np
from datetime import datetime as dt


#%% Functions

def clean(df, rowload=True):
    df = clean_column_names(df)
    df = clean_data(df)
    df.dropna(how='all', axis=0, inplace=True)
    df.dropna(how='all', axis=1, inplace=True)
    if rowload == True:
        df['RowLoadDateTime'] = dt.now()
    return df


def clean_data(df):
    df = df.applymap(__scrub_data)
    return df


def clean_column_names(df):
    df.columns = __scrub_column_names(df.columns)
    return df


#%% Internal Functions

def __scrub_data(x):
    if isinstance(x, str):
        x = x.strip()
    elif isinstance(x, int) or isinstance(x, float):
        if x == 0:
            x = np.NaN
    return x


def __scrub_column_names(lst_input):
    lst_output = []
    for el in lst_input:
        el_new = ''
        for sc in [' ', '\\n']:
            el = el.replace(sc, '_')
        for ch in el:
            if ((ch.lower()>='a' and ch.lower()<='z') or (ch>='0' and ch<='9') or ch=='_'):
                el_new += ch
        if el_new[0] == '_':
            el_new = el_new[1:]
        if el_new[-1] == '_':
            el_new = el_new[:-1]
        lst_output.append(el_new)
    return lst_output
