
import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.helpers import query_to_df

def fill_levels(df, pk_id):
    for level in ['sector_id', 'subsector_id', 'industry_group_id', 'ent_id', 'country_id']:
        if pk_id != level:
            df[level] = 0
    return df

def clean_tables(table):
    db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))
    query = 'DROP TABLE {}'.format(table)
    query_to_df(db_connector, raw_query=query)
    print('Success! {}'.format(table))

    return 0

def format_text(df, cols_names=None, stopwords=None):
    # format
    for ele in cols_names:
        df[ele] = df[ele].str.title().str.strip()
        for ene in stopwords:
            df[ele] = df[ele].str.replace(' ' + ene.title() + ' ', ' ' + ene + ' ')

    return df

def representative_values(df, target_col, target_val, target_threshold=10):
    count = df.shape[0]
    value = len(list(df.loc[df[target_col] == target_val, target_col]))
    #print(count, value, 'temp')
    result = round((value/count)*100, 3)
    if result > target_threshold:
        return [target_col, target_threshold, result, False]
    else:
        return [target_col, target_threshold, result, True]

def validate_category(df, dim_column, target_column, target_value, threshold=0.1):
    """dim_column: 'sector_id'
       target_column: 'value_c'
       target_value: 'c'
    """
    temp = df.copy()
    for sector in list(df[dim_column].unique()):
        temp[target_column] = temp[target_column].astype(str).str.lower()
        count = temp.loc[temp[dim_column] == sector, target_column].shape[0]
        value = len(list(temp.loc[(temp[dim_column] == sector) & (temp[target_column] == target_value), target_column]))
        result = round((value/count)*100, 3)
        if result > 10:
            #print('result: {}%, drop: {}, ID: {}'.format(result, True, sector))
            temp = temp.loc[temp[dim_column] != sector].copy()
        else:
            #print('result: {}%, drop: {}, ID: {}'.format(result, False, sector))
            pass
    
    #print('init: {}, end: {}'.format(df.shape[0], temp.shape[0]))
    return temp

def check_confidentiality(df, level, geo, industry, industry_pk, confidential_column, confidential_value, value):
    query = list(df.loc[(df[level] == geo) & (df[industry_pk] == industry), confidential_column])
    try:
        test = query.count('C')/len(query)
    except ZeroDivisionError:
        test = 0
    if test > 0.1:
        return 'C'
    else:
        return value