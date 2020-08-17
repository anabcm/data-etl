
import os
import json
import requests
import pandas as pd

def find_missing_values():

    base_url = os.environ.get('BASE_URL')

    url = '{}/data.jsonrecords?Country=mex&cube=complexity_eci_a_hs12_hs6&drilldowns=Country%2CYear%2CECI+Rank&measures=ECI&parents=false&sparse=false'.format(base_url)

    r = requests.get(url)
    data = r.json()["data"]

    df = pd.DataFrame(data)
    df.columns = df.columns.str.lower()
    df.columns = df.columns.str.replace(' ', '_')

    df = df[['eci', 'eci_rank', 'year']].copy()

    for col in ['ent_id', 'mun_id', 'zm_id', 'time_id', 'latest']:
        df[col] = 0

    df.loc[df['year'] == df['year'].max(), 'latest'] = 1

    df['level'] = 'Nation'
    
    return df