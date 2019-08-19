import pandas as pd
from bamboo_lib.models import PipelineStep
from bamboo_lib.models import EasyPipeline
from bamboo_lib.connectors.models import Connector
from bamboo_lib.steps import LoadStep

class ReadStep(PipelineStep):
    def run_step(self, prev, params):
        # income range data
        url = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vR08Js9Sh4nNTMe5uBcsDUFedG5MOjIf90p6EHAr1_sWY5kpnI3xUvyPHzQpTEUrXz1pskaoc0uyea6/pub?output=xlsx'
        df = pd.read_excel(url, dtype='str', sheet_name='income', usecols=[0,1,4])
        df.id = df.id.astype('int')
        return df

class CoveragePipeline(EasyPipeline):
        @staticmethod
        def description():
            return 'ETL Income dim table'
    
        @staticmethod
        def steps(params):
            # Use of connectors specified in the conns.yaml file
            db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))
            dtype = {
                'id':      'UInt8',
                'name_en': 'String',
                'name_es': 'String',
            }
    
            read_step = ReadStep()
    
            load_step = LoadStep(
                'dim_shared_income', db_connector, if_exists='drop', pk=['id'], dtype=dtype
            )
    
            return [read_step, load_step]