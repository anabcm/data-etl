
import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, PipelineStep, Parameter
from bamboo_lib.steps import LoadStep

class ReadStep(PipelineStep):
    def run_step(self, prev, params):
        # read data
        url = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vTzv8dN6-Cn7vR_v9UO5aPOBqumAy_dXlcnVOFBzxCm0C3EOO4ahT5FdIOyrtcC7p-akGWC_MELKTcM/pub?output=xlsx'
        df = pd.read_excel(url, sheet_name='countries')
        df.code = df.code.str.lower()

        return df

class DimCountriesPipeline(EasyPipeline):
    @staticmethod
    def steps(params):
        
        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))
        
        dtype = {
            'code': 'String',
            'name_en': 'String',
            'name_es': 'String',
        }
        
        read_step = ReadStep()
        load_step = LoadStep('dim_shared_countries', db_connector, if_exists='drop', pk=['code'], dtype=dtype)

        return [read_step, load_step]