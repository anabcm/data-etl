import pandas as pd
import gspread
import time
from helpers import query_to_df
from bamboo_lib.helpers import grab_connector
from oauth2client.service_account import ServiceAccountCredentials

### Data
db_connector = grab_connector("../conns.yaml", "clickhouse-database")
query = "SELECT * from dim_shared_careers_anuies"
df = query_to_df(db_connector, query, table_name='dim_shared_careers_anuies')

for col in ['code', 'area']:
    df[col] = df[col].astype('str').astype('float')

### Translation
df['name_en'] = range(2, df.shape[0]+2)
df['name_en'] = '=GOOGLETRANSLATE(B' + df['name_en'].astype('str') + ', "es", "en")' 

# special cases
df.loc[df.name_es.str.contains('“'), 'name_es'] = df.loc[df.name_es.str.contains('“'), 'name_es'].str.replace('“L2”', '"L2"')
df.loc[df.name_es.str.contains('–'), 'name_es'] = df.loc[df.name_es.str.contains('–'), 'name_es'].str.replace('–', ' - ').str.replace('  ', ' ')

### Insert into spreadsheet
scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name('drive-api.json', scope)
client = gspread.authorize(creds)

### write all data in spread sheet id, need to be shared with drive-api.json mail
client.import_csv('1XTmWXgMqzs4FJlgbRcatw-q3ksFVId0r6cuAmg9pOPQ', df.to_csv(index=False, encoding='utf-8'))