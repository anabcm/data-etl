import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, PipelineStep, Parameter
from bamboo_lib.steps import LoadStep

class ReadStep(PipelineStep):
    def run_step(self, prev, params):
        # read data
        url = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vTzv8dN6-Cn7vR_v9UO5aPOBqumAy_dXlcnVOFBzxCm0C3EOO4ahT5FdIOyrtcC7p-akGWC_MELKTcM/pub?output=xlsx'
        df = pd.read_excel(url, sheet_name='institutions', dtypes='str')

        return df

class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        df = prev

        # type conversion
        for col in ['sostenimiento', 'campus']:
            df[col] = df[col].astype('float')
        
        return df

class EnrollmentPipeline(EasyPipeline):
    @staticmethod
    def steps(params):
        
        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))
        
        dtype = {
            'work_center_id':   'String',
            'work_center_name': 'String',
            'campus':           'UInt16',
            'sostenimiento':    'UInt8'
        }
        
        read_step = ReadStep()
        transform_step = TransformStep()
        load_step = LoadStep('dim_shared_work_centers', db_connector, if_exists='append', 
                            pk=['work_center_id'], dtype=dtype, engine='ReplacingMergeTree')

        return [read_step, transform_step, load_step]