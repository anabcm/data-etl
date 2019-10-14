def format_text(df, cols_names=None, stopwords=None):

    # format
    for ele in cols_names:
        df[ele] = df[ele].str.title().str.strip()
        for ene in stopwords:
            df[ele] = df[ele].str.replace(' ' + ene.title() + ' ', ' ' + ene + ' ')

    return df

def create_index(df, col, target):
    ran = dict(zip(df[col].unique(), range(1, len(df[col].unique()) + 1)))
    df[target] = df[col]
    df[target].replace(ran, inplace=True)
    return df

def query_to_df(connector_obj, raw_query, table_name):
    import pandas as pd
    result = connector_obj.raw_query(raw_query)
    # default column names
    try:
        columns = connector_obj.raw_query(('describe {}').format(table_name))
        columns = [x[0] for x in columns]
    except:
        return print('Table does not exist.')

    return pd.DataFrame(result, columns=columns)

#grouped index
def gouped_index(df, column=None, objetive='id'):
    df[objetive] = None
    for area in df[column].str[:2].unique():
        ran = range(1, df.loc[df[column].str[:2] == area, column].shape[0] + 1)
        df.loc[df[column].str[:2] == area, objetive] = ran
    return df

def word_case(series, target, inplace=False):
    # upper() fixed
    try:       
        unique_rows = series.loc[series.str.contains(target)].unique()
        for ele in unique_rows:
            val = ele.split()
            for v in range(len(val)):
                if val[v].lower() == target.lower():
                    val[v] = val[v].upper()
            if inplace:
                series.loc[series == ele] = ' '.join(val).strip()
            else:
                return ' '.join(val).strip()
    except:
        return 'Target: {} not found'.format(target)