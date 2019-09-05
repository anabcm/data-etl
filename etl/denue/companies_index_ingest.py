
import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, PipelineStep, Parameter
from bamboo_lib.steps import LoadStep

class ReadStep(PipelineStep):
    def run_step(self, prev, params):
        url = 'https://storage.googleapis.com/datamexico-data/denue/companies_index.csv'
        df = pd.read_csv(url, encoding='latin-1')
        df.fecha_alta = df.fecha_alta.str.replace('-', '')
        return df

class DENUECompaniesPipeline(EasyPipeline):
    @staticmethod
    def steps(params):
        
        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))
        
        dtypes = {
            'id':          'UInt32',
            'nom_estab':   'String',
            'raz_social':  'String',
            'date':        'UInt32',
            'fecha_alta':  'UInt32'
        }

        read_step = ReadStep()
        load_step = LoadStep('inegi_denue_companies', connector=db_connector, if_exists='drop', pk=['id'], dtype=dtypes)
        return [read_step, load_step]