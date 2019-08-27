import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, PipelineStep, Parameter
from bamboo_lib.steps import LoadStep

class ReadStep(PipelineStep):
    def run_step(self, prev, params):
        # read data
        url = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vTzv8dN6-Cn7vR_v9UO5aPOBqumAy_dXlcnVOFBzxCm0C3EOO4ahT5FdIOyrtcC7p-akGWC_MELKTcM/pub?output=xlsx'
        df = pd.read_excel(url, sheet_name='institutions', dtypes='str')
        ent = pd.read_excel(url, sheet_name='origin', dtypes='str')

        return df, ent

class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        df, ent = prev[0], prev[1]
        # type format
        df['mun_id'] = df['mun_id'].astype('str').str.zfill(3)

        # ids replace from external table
        df.ent_id = df.ent_id.str.title()
        df.ent_id.replace(dict(zip(ent.origin, ent.id)), inplace=True)
        
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