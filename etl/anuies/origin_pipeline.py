import nltk
import pandas as pd
from helpers import format_text
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, PipelineStep, Parameter
from bamboo_lib.steps import LoadStep

class ReadStep(PipelineStep):
    def run_step(self, prev, params):
        # read data
        df = pd.read_excel(params.get('url'), header=1)
        df.columns = df.columns.str.lower()
        df.rename(columns={'entidad': 'ent_id', 'municipio': 'mun_id', 'cve campo unitario': 'career', 
                           'nivel': 'type', 'ciclo': 'period', 'clave centro de trabajo': 'campus_id', 
                           'nombre carrera sep': 'program'}, inplace=True)
        # external ids
        url = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vTzv8dN6-Cn7vR_v9UO5aPOBqumAy_dXlcnVOFBzxCm0C3EOO4ahT5FdIOyrtcC7p-akGWC_MELKTcM/pub?output=xlsx'
        ent = pd.read_excel(url, sheet_name='origin', dtypes='str')
        careers = pd.read_csv('https://docs.google.com/spreadsheets/d/e/2PACX-1vSry9xO5_KVDA7bWVTKPrFgjnDbU6CP6f9lNrGX5zqfJH3HBc2-3EPeIBfCS92_UyiSnBnt5XeEpb2T/pub?output=csv')
        return df, ent, careers

class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        df, ent, careers = prev[0], prev[1], prev[2]
        # type format
        for col in ['ent_id', 'mun_id', 'career', 'type', 'period', 'campus_id']:
            df[col] = df[col].ffill()
        df.ent_id = df.ent_id.str.title()

        # ids replace from external table
        df.ent_id.replace(dict(zip(ent.origin, ent.id)), inplace=True)

        # municipality level id
        try:
            df.loc[:, 'mun_id'] = df.loc[:, 'ent_id'].astype('str') + df.loc[:, 'mun_id'].astype('int').astype('str').str.zfill(3)
        except:
            df.loc[:, 'mun_id'] = df.loc[:, 'ent_id'].astype('str') + df.loc[:, 'mun_id'].astype('str')
        df.drop(columns=['ent_id'], inplace=True)

        # totals clean
        df.career = df.career.astype('str')
        for col in ['mun_id', 'career', 'type', 'period', 'campus_id', 'program']:
            df = df.loc[df[col].str.contains('Total') == False].copy()
            df[col] = df[col].str.strip().str.replace('  ', ' ').str.replace(':', '')
        df.career = df.career.str.replace('.', '').astype('int')

        # column names format
        df.columns = df.columns.str.replace('suma de ', '')

        # melt step
        df = df[['mun_id', 'career', 'type', 'period', 'campus_id', 'program',
                 'pni-agu', 'pni-bc', 'pni-bcs', 'pni-cam', 'pni-coa',
                 'pni-col', 'pni-chia', 'pni-chih', 'pni-df', 'pni-dur', 'pni-gua',
                 'pni-gue', 'pni-hid', 'pni-jal', 'pni-mex', 'pni-mic', 'pni-mor',
                 'pni-nay', 'pni-nl', 'pni-oax', 'pni-pue', 'pni-que', 'pni-qr',
                 'pni-slp', 'pni-sin', 'pni-son', 'pni-tab', 'pni-tam', 'pni-tla',
                 'pni-ver', 'pni-yuc', 'pni-zac']].copy()

        # column names format
        df.columns = df.columns.str.replace('pni-', '')

        df = df.melt(id_vars=['mun_id', 'career', 'type', 'period', 'campus_id', 'program'], var_name='origin', value_name='value')
        df = df.loc[df.value != 0]

        # external ids transfomation
        df['origin'].replace(dict(zip(ent.code, ent.id.astype('int'))), inplace=True)

        types = {
            'TS': 8,
            'LEN': 10,
            'LUT': 11,
            'ESPECIALIDAD': 12,
            'MAESTRÍA': 13,
            'DOCTORADO': 14
        }
        df.type.replace(types, inplace=True)

        # careers ids
        operations = {
            '“L2”': '"L2"',
            '–': '-'
        }
        for k, v in operations.items():
            df.program = df.program.str.replace(k, v)
        
        # stopwords es
        nltk.download('stopwords')
        df = format_text(df, ['program'], stopwords=nltk.corpus.stopwords.words('spanish'))
        df.program.replace(dict(zip(careers.name_es, careers.code)), inplace=True)

        for col in ['mun_id', 'career', 'program', 'type', 'origin', 'value']:
            df[col] = df[col].astype('float')
        
        df.drop(columns=['career', 'period'], inplace=True)

        df['year'] = params.get('period')
        
        return df

class OriginPipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return [
            Parameter(name='url', dtype=str),
            Parameter(name='period', dtype=int)
        ]

    @staticmethod
    def steps(params):
        
        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))
        
        dtype = {
            'mun_id':      'UInt16',
            'type':        'UInt8',
            'year':        'UInt16',
            'campus_id':   'String',
            'program':     'UInt64',
            'origin':      'UInt8',
            'value':       'UInt32',
        }
        
        read_step = ReadStep()
        transform_step = TransformStep()
        load_step = LoadStep('anuies_origin', db_connector, if_exists='append', pk=['mun_id', 'campus_id', 'program', 'year'], dtype=dtype)

        return [read_step, transform_step, load_step]