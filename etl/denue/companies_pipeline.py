import glob
import pandas as pd

from google.cloud import storage
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, PipelineStep
from bamboo_lib.steps import LoadStep

class ReadStep(PipelineStep):
    def run_step(self, prev, params):
        storage_client = storage.Client.from_service_account_json('datamexico.json')
        bucket = storage_client.get_bucket('datamexico-data')
        blobs = bucket.list_blobs()
        urls = []
        for blob in blobs:
            if 'denue' in blob.name:
                urls.append('https://storage.googleapis.com/datamexico-data/' + str(blob.name))
        
        return urls

class TransformStep(PipelineStep):
    def run_step(self, prev, params):

        csv = prev
        # read data
        df = pd.DataFrame()
        df_temp = pd.DataFrame()
        for val in range(len(csv)):
            df_temp = pd.read_csv(csv[val], encoding='utf-8', dtype='str', usecols=['nom_estab', 'codigo_act', 
                    'per_ocu', 'cod_postal', 'cve_ent',
                   'cve_mun', 'cve_loc', 'ageb', 'manzana', 
                   'tipoUniEco', 'fecha_alta', 'latitud', 'longitud'])
            df = df.append(df_temp, sort=False)

        # format, create columns
        df.columns = df.columns.str.lower()
        
        df.cve_mun = df.cve_ent + df.cve_mun
        df.cve_loc = df.cve_mun + df.cve_loc

        df['cve_loc'] = df['cve_loc'].astype('int')

        df.drop(columns=['ageb', 'manzana', 'cve_ent', 'cve_mun'], inplace=True)
        
        df.per_ocu = df.per_ocu.str.replace('personas', '').str.strip()
        df.per_ocu = df.per_ocu.str.replace(' a ', ' - ')
        df.per_ocu = df.per_ocu.str.replace(' y más', ' +')
        
        # replace values
        workers = {
            '0 - 5': 1,
            '6 - 10': 2,
            '11 - 30': 3,
            '31 - 50': 4,
            '51 - 100': 5,
            '101 - 250': 6,
            '251 +': 7
        }
        df.per_ocu.replace(workers, inplace=True)

        place = {
            'Fijo': 1,
            'Semifijo': 2,
            'Actividad en vivienda': 3
        }
        df.tipounieco.replace(place, inplace=True)

        # rename column names
        column_names = {
            'nom_estab': 'name',
            'codigo_act': 'national_industry_id',
            'per_ocu': 'n_workers',
            'cod_postal': 'postal_code',
            'tipounieco': 'establishment',
            'fecha_alta': 'directory_added_date',
            'cve_loc': 'loc_id',
            'latitud': 'latitude',
            'longitud': 'longitude'
        }
        df.rename(columns=column_names, inplace=True)
        
        # data types conversion
        dtypes = {
            'name': 'str',
            'national_industry_id': 'str',
            'directory_added_date': 'str',
            'n_workers': 'int',
            'postal_code': 'int',
            'loc_id': 'int',
            'establishment': 'int',
            'latitude': 'float',
            'longitude': 'float'
        }
        
        for key, val in dtypes.items():
            try:
                # string column check
                if (df.loc[:, key].isnull().sum() > 0) & (key == 'str'):
                    df.loc[:, key] = df.loc[:, key].astype('object')
                else:
                    df.loc[:, key] = df.loc[:, key].astype(val)
            except Exception as e:
                if val == 'int':
                    df.loc[:, key] = df.loc[:, key].astype('float')
                else:
                    print(e)
        
        df['publication_date'] = '2019-10-04'

        return df

class CoveragePipeline(EasyPipeline):
    @staticmethod
    def steps(params):
        
        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))
        
        dtypes = {
            'name':                 'String',
            'national_industry_id': 'String',
            'n_workers':            'UInt8',
            'postal_code':          'UInt32',
            'establishment':        'UInt8',
            'loc_id':               'UInt32',
            'latitude':             'Float32',
            'longitude':            'Float32'
        }

        read_step = ReadStep()
        transform_step = TransformStep()
        load_step = LoadStep('inegi_denue', connector=db_connector, if_exists='drop', pk=['loc_id', 'national_industry_id'], dtype=dtypes, 
                                nullable_list=['name', 'n_workers', 'postal_code', 'establishment', 'latitude', 'longitude', 'directory_added_date'])
        return [read_step, transform_step, load_step]