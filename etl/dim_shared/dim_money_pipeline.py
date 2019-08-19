import pandas as pd
from bamboo_lib.models import PipelineStep
from bamboo_lib.models import EasyPipeline
from bamboo_lib.connectors.models import Connector
from bamboo_lib.steps import LoadStep

class ReadStep(PipelineStep):
    def run_step(self, prev, params):
        # income range data
        url = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vTieVnRovfP7AOMtqxIJcrFl8Tayz6Irz-Bc1en1NSIKtjjPtGaBRaCaSeePRrpQMmHMzSt2VO93Wav/pub?output=csv'
        df = pd.read_csv(url, dtype='str')

        df['id'] = df.id.astype(int)
        df['enigh_group_id'] = df['enigh_group_id'].astype(int)

        df = df[['id', 'enigh_group_id', 'enigh_group', 'name_en', 'name_es']]
        return df

class DimMoneyPipeline(EasyPipeline):
        @staticmethod
        def description():
            return 'ETL money dim table'
    
        @staticmethod
        def steps(params):
            # Use of connectors specified in the conns.yaml file
            db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))
            dtype = {
                'id':               'UInt8',
                'enigh_group_id':   'UInt8'
                'enigh_group':      'String',
                'name_en':          'String',
                'name_es':          'String',
            }
    
            read_step = ReadStep()
    
            load_step = LoadStep(
                'dim_shared_money', db_connector, if_exists='drop', pk=['id'], dtype=dtype
            )
    
            return [read_step, load_step]