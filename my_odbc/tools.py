
import numpy as np
from datetime import datetime as dt


#%% Functions

def clean(data):
    def clean_data(x):
        if isinstance(x, str):
            x = x.strip()
        elif isinstance(x, int) or isinstance(x, float):
            if x == 0:
                x = np.NaN
        return x

    def clean_column_names(lst_input):
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

    data.columns = clean_column_names(data.columns)
    data = data.applymap(clean_data)
    data.dropna(how='all', axis=0, inplace=True)
    data.dropna(how='all', axis=1, inplace=True)
    data['RowLoadDateTime'] = dt.now()
    return data
