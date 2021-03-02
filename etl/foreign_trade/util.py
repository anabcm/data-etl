
import pandas as pd
from bamboo_lib.helpers import query_to_df
from bamboo_lib.connectors.models import Connector

def check_update(files, table):
    db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))

    current_files = []
    for level in ['ent', 'mun']:
        try:
            query = 'SELECT distinct(url) FROM {}{}'.format(table, level)
            temp = list(query_to_df(db_connector, raw_query=query)['url'])
        except:
            temp = []
        current_files = current_files + temp

    if len(current_files) > 0:
        df = pd.DataFrame({'url': files})
        return list(df.loc[~df['url'].isin(current_files), 'url'])
    else:
        return files

LEVELS = {'National':  ['UInt8',  'ent', 0], 
          'State':     ['UInt8',  'ent', 1], 
          'Municipal': ['UInt16', 'mun', 2]}

DEPTHS = {'HS_2D': 'hs2_id', 
          'HS_4D': 'hs4_id',
          'HS_6D': 'hs6_id'}

def hs6_converter(hs6):
    """Adds chaper information to HS6 code"""
    leading2 = int(hs6[:2])
    if leading2 <= 5: return "{}{}".format("01", hs6)
    if leading2 <= 14: return "{}{}".format("02", hs6)
    if leading2 <= 15: return "{}{}".format("03", hs6)
    if leading2 <= 24: return "{}{}".format("04", hs6)
    if leading2 <= 27: return "{}{}".format("05", hs6)
    if leading2 <= 38: return "{}{}".format("06", hs6)
    if leading2 <= 40: return "{}{}".format("07", hs6)
    if leading2 <= 43: return "{}{}".format("08", hs6)
    if leading2 <= 46: return "{}{}".format("09", hs6)
    if leading2 <= 49: return "{}{}".format("10", hs6)
    if leading2 <= 63: return "{}{}".format("11", hs6)
    if leading2 <= 67: return "{}{}".format("12", hs6)
    if leading2 <= 70: return "{}{}".format("13", hs6)
    if leading2 <= 71: return "{}{}".format("14", hs6)
    if leading2 <= 83: return "{}{}".format("15", hs6)
    if leading2 <= 85: return "{}{}".format("16", hs6)
    if leading2 <= 89: return "{}{}".format("17", hs6)
    if leading2 <= 92: return "{}{}".format("18", hs6)
    if leading2 <= 93: return "{}{}".format("19", hs6)
    if leading2 <= 96: return "{}{}".format("20", hs6)
    if leading2 <= 97: return "{}{}".format("21", hs6)
    if leading2 <= 99: return "{}{}".format("22", hs6)
    return "{}{}".format("xx", hs6)

def get_time(url):
    # month
    import re
    try:
        regex = re.findall('[0-9]{4}(?![0-9])', url)[0]
        month_id = int('20' + regex[2:] + regex[:2])
        data = {
            'month_id': month_id,
            'year': 0
        }
        return data
    # annual
    except:    
        regex = re.findall('[0-9]{2}(?![0-9])', url)[0]
        year = int('20' + regex)
        data = {
            'month_id': 0,
            'year': year
        }
        return data

def get_level(url, levels):
    for k, v in levels.items():
        if k in url:
            return v[0], v[1], v[2]
        else:
            continue
    return None

def get_depth(url, depths):
    for k, v in depths.items():
        if k in url:
            return v
        else:
            continue
    return None

def get_params(url, levels=LEVELS, depths=DEPTHS):
    time = []
    level = get_level(url, levels)
    depth = get_depth(url, depths)
    time = get_time(url)
    data = {
        'level': level,
        'depth': depth,
        'datetime': time,
    }
    return data

def get_number(val):
    import re
    return int(re.findall('[0-9]{1}(?![0-9])', val)[0])