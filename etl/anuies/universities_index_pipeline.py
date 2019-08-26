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
        df.columns = ['mun_id', 'ent_id', 'work_center_id', 'work_center_name', 'sostenimiento', 'campus']
        # external ids
        url = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vTzv8dN6-Cn7vR_v9UO5aPOBqumAy_dXlcnVOFBzxCm0C3EOO4ahT5FdIOyrtcC7p-akGWC_MELKTcM/pub?output=xlsx'
        ent = pd.read_excel(url, sheet_name='origin', dtypes='str')
        camp = pd.read_excel(url, sheet_name='campus', dtypes='str')

        return df, ent, camp

class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        df, ent, camp = prev[0], prev[1], prev[2]
        # type format
        for col in ['mun_id', 'ent_id']:
            df[col] = df[col].ffill()

        # encoding
        sostenimiento = {
            'PÚBLICO': 1,
            'PARTICULAR': 2
        }
        df.sostenimiento.replace(sostenimiento, inplace=True)
        
        # stopwords
        stopwords_es = ['a', 'e', 'ante', 'con', 'contra', 'de', 'desde', 'la', 'lo', 'las', 'los', 'y']

        cols_es = ['work_center_name', 'campus']

        #format
        df = format_text(df, cols_names=cols_es, stopwords=stopwords_es)

        # ids replace from external table
        df.ent_id = df.ent_id.str.title()
        df.ent_id.replace(dict(zip(ent.origin, ent.id)), inplace=True)
        df.campus = df.campus.str.replace(',', '').str.replace('  ', ' ')
        df.campus.replace(dict(zip(camp.name, camp.id)), inplace=True)
        
        # minicipality id
        df.mun_id = df.mun_id.astype('int').astype('str').str.zfill(3)
        df.loc[:, 'mun_id'] = df.ent_id.astype('str') + df.mun_id
        df.drop(columns='ent_id', inplace=True)

        # type conversion
        for col in ['mun_id', 'sostenimiento', 'campus']:
            df[col] = df[col].astype('float')
        
        return df

class EnrollmentPipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return [
            Parameter(name='url', dtype=str)
        ]

    @staticmethod
    def steps(params):
        
        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))
        
        dtype = {
            'mun_id':           'UInt16',
            'work_center_id':   'String',
            'work_center_name': 'String',
            'campus':           'UInt16',
            'sostenimiento':    'UInt8'
        }
        
        read_step = ReadStep()
        transform_step = TransformStep()
        load_step = LoadStep('dim_shared_work_centers', db_connector, if_exists='append', 
                            pk=['mun_id', 'work_center_id'], dtype=dtype, engine='ReplacingMergeTree')

        return [read_step, transform_step, load_step]