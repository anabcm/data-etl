def format_text(df, cols_names=None, stopwords=None):

    # format
    for ele in cols_names:
        df[ele] = df[ele].str.title().str.strip()
        for ene in stopwords:
            df[ele] = df[ele].str.replace(' ' + ene.title() + ' ', ' ' + ene + ' ')

    return df

import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, PipelineStep, Parameter
from bamboo_lib.steps import LoadStep

class ReadStep(PipelineStep):
    def run_step(self, prev, params):
        # read data
        df = pd.read_excel(params.get('url'), header=1)
        df.columns = df.columns.str.lower()
        df.rename(columns={'entidad': 'ent_id', 'municipio': 'mun_id', 'cve campo unitario': 'career', 
                           'nivel': 'type', 'ciclo': 'period', 'clave centro de trabajo': 'institution', 
                           'nombre carrera sep': 'program'}, inplace=True)
        df.drop(columns=['ent_id', 'mun_id'], inplace=True)
        # external ids
        url = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vTzv8dN6-Cn7vR_v9UO5aPOBqumAy_dXlcnVOFBzxCm0C3EOO4ahT5FdIOyrtcC7p-akGWC_MELKTcM/pub?output=xlsx'
        ent = pd.read_excel(url, sheet_name='origin', dtypes='str')
        careers = pd.read_excel(url, sheet_name='careers')
        return df, ent, careers

class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        df, ent, careers = prev[0], prev[1], prev[2]
        # type format
        for col in ['career', 'type', 'period', 'institution']:
            df[col] = df[col].ffill()

        # totals clean
        df.career = df.career.astype('str')
        for col in ['career', 'type', 'period', 'institution', 'program']:
            df = df.loc[df[col].str.contains('Total') == False].copy()
            df[col] = df[col].str.strip().str.replace('  ', ' ').str.replace(':', '')
        df.career = df.career.str.replace('.', '').astype('int')

        # column names format
        df.columns = df.columns.str.replace('suma de ', '')

        # melt step
        df = df[['career', 'type', 'period', 'institution', 'program',
                 'pni-agu', 'pni-bc', 'pni-bcs', 'pni-cam', 'pni-coa',
                 'pni-col', 'pni-chia', 'pni-chih', 'pni-df', 'pni-dur', 'pni-gua',
                 'pni-gue', 'pni-hid', 'pni-jal', 'pni-mex', 'pni-mic', 'pni-mor',
                 'pni-nay', 'pni-nl', 'pni-oax', 'pni-pue', 'pni-que', 'pni-qr',
                 'pni-slp', 'pni-sin', 'pni-son', 'pni-tab', 'pni-tam', 'pni-tla',
                 'pni-ver', 'pni-yuc', 'pni-zac']].copy()

        # column names format
        df.columns = df.columns.str.replace('pni-', '')

        df = df.melt(id_vars=['career', 'type', 'period', 'institution', 'program'], var_name='origin', value_name='value')
        df = df.loc[df.value != 0]

        # external ids transfomation
        df['origin'].replace(dict(zip(ent.code, ent.id.astype('int'))), inplace=True)

        types = {
            'TS': 1,
            'LEN': 2,
            'LUT': 3,
            'MAESTRÍA': 4,
            'ESPECIALIDAD': 5,
            'DOCTORADO': 6
        }
        df.type.replace(types, inplace=True)

        # careers ids
        stopwords_es = ['a', 'e', 'en', 'ante', 'con', 'contra', 'de', 'del', 'desde', 'la', 'lo', 'las', 'los', 'y']
        df = format_text(df, ['program'], stopwords=stopwords_es)
        df.program.replace(dict(zip(careers.name_es, careers.code)), inplace=True)

        for col in ['career', 'program', 'type', 'origin', 'value']:
            df[col] = df[col].astype('float')
        
        df.drop(columns=['career'], inplace=True)
        
        return df

class OriginPipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return [
            Parameter(name='url', dtype=str)
        ]

    @staticmethod
    def steps(params):
        
        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))
        
        dtype = {
            'type':        'UInt8',
            'period':      'String',
            'institution': 'String',
            'program':     'UInt64',
            'origin':      'UInt8',
            'value':       'UInt32',
        }
        
        read_step = ReadStep()
        transform_step = TransformStep()
        load_step = LoadStep('anuies_postgraduate_origin', db_connector, if_exists='append', pk=['institution', 'program'], dtype=dtype)

        return [read_step, transform_step, load_step]