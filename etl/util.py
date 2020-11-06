
import os
import json
import requests
import pandas as pd

REVISION_MAP = {
    'hs92':     0,
    'hs96':     1,
    'hs02':     2,
    'hs07':     3,
    'hs12':     4,
    'hs17':     5,
}

def hs6_converter(hs6):
    """Adds section information to HS6 code"""
    try:
        leading2 = int(hs6[:2])
    except TypeError:
        raise ValueError()

    # This is a hack since there are some pipelines that use this helper
    # while providing a non-HS6 ID
    hs6_len = len(str(hs6))
    nes_id = '22' + '9' * hs6_len

    if leading2 == 0: return nes_id
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
    if leading2 <= 99: return nes_id

    return nes_id

def find_missing_values():

    base_url = os.environ.get('BASE_URL')

    url = '{}/data.jsonrecords?Country=mex&cube=complexity_eci_a_hs12_hs6&drilldowns=Country%2CYear%2CECI+Rank&measures=ECI&parents=false&sparse=false'.format(base_url)

    r = requests.get(url)
    data = r.json()["data"]

    df = pd.DataFrame(data)
    df.columns = df.columns.str.lower()
    df.columns = df.columns.str.replace(' ', '_')

    df = df[['eci', 'eci_rank', 'year']].copy()
    df.columns = ['eci', 'eci_ranking', 'year']

    for col in ['ent_id', 'mun_id', 'zm_id', 'time_id', 'latest']:
        df[col] = 0

    df.loc[df['year'] == df['year'].max(), 'latest'] = 1

    df['level'] = 'Nation'

    df['nation_id'] = 'mex'
    
    return df