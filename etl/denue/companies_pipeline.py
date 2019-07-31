import glob
import pandas as pd

from google.cloud import storage
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, PipelineStep, Parameter
from bamboo_lib.steps import LoadStep

class ReadStep(PipelineStep):
    def run_step(self, prev, params):
        # read data
        df = pd.read_csv(params['url'], encoding='utf-8', dtype='str', usecols=usecols=[0, 1, 3, 5, 25, 26, 28, 30, 32, 33, 37, 38, 39, 40])
        return df

class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        df = prev

        # format, create columns
        df.columns = df.columns.str.lower()
        
        df.cve_mun = df.cve_ent + df.cve_mun
        df.cve_loc = df.cve_mun + df.cve_loc

        df['cve_loc'] = df['cve_loc'].astype('int')

        df.drop(columns=['ageb', 'manzana', 'cve_ent', 'cve_mun'], inplace=True)
        
        df.nom_estab = df.nom_estab.str.strip()

        df.per_ocu = df.per_ocu.str.replace('personas', '').str.strip()
        df.per_ocu = df.per_ocu.str.replace(' a ', ' - ')
        df.per_ocu = df.per_ocu.str.replace(' y más', ' +')
        df.fecha_alta = df.fecha_alta.str.replace('-', '')

        # date processing
        df.fecha_alta = df.fecha_alta.str.upper()
        months = {'ENERO': '01',
                'FEBRERO': '02',
                'MARZO': '03',
                'ABRIL': '04',
                'MAYO': '05',
                'JUNIO': '06',
                'JULIO': '07',
                'AGOSTO': '08',
                'SEPTIEMBRE': '09',
                'OCTUBRE': '10',
                'NOVIEMBRE': '11',
                'DICIEMBRE': '12'}
        for key, val in months.items():
            for date in df.fecha_alta.unique().tolist():
                if key in date:
                    temp = date.replace(key, val).split()[1] + date.replace(key, val).split()[0]
                    df.fecha_alta = df.fecha_alta.str.replace(date, temp)

        #range creation
        df['lower'] = pd.np.nan
        df['upper'] = pd.np.nan
        df['middle'] = pd.np.nan

        for ele in df.per_ocu.unique():
            try:
                if '-' in ele:
                    df.loc[df.per_ocu == ele, 'lower'] = int(ele.split(' - ')[0])
                    df.loc[df.per_ocu == ele, 'upper'] = int(ele.split(' - ')[1])
                    df.loc[df.per_ocu == ele, 'middle'] = (float(ele.split(' - ')[1]) + float(ele.split(' - ')[0]))/2.0
                else:
                    df.loc[df.per_ocu == ele, 'lower'] = int(ele.split(' +')[0])
                    df.loc[df.per_ocu == ele, 'upper'] = int(ele.split(' +')[0])
                    df.loc[df.per_ocu == ele, 'middle'] = int(ele.split(' +')[0])
            except:
                continue
        
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
            'directory_added_date': 'int',
            'n_workers': 'int',
            'postal_code': 'int',
            'loc_id': 'int',
            'establishment': 'int',
            'latitude': 'float',
            'longitude': 'float',
            'lower': 'int',
            'middle': 'float',
            'upper': 'int'
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
        
        df['publication_date'] = params['date'].astype('int')

        return df

class CoveragePipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return [
            Parameter(label="Date", name="date", dtype=str),
            Parameter(label="URL", name="url", dtype=str)
        ]

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
            'longitude':            'Float32',
            'lower':                'UInt8',
            'middle':               'Float32',
            'upper':                'UInt8',
            'publication_date':     'UInt32',
            'directory_added_date': 'UInt32'
        }

        read_step = ReadStep()
        transform_step = TransformStep()
        load_step = LoadStep('inegi_denue', connector=db_connector, if_exists='append', pk=['id', 'loc_id', 'national_industry_id'], dtype=dtypes, 
                                nullable_list=['name', 'n_workers', 'postal_code', 'establishment', 'latitude', 'longitude', 'directory_added_date',
                                'lower', 'middle', 'upper'])
        return [read_step, transform_step, load_step]