import pandas as pd
from bamboo_lib.models import PipelineStep, EasyPipeline
from bamboo_lib.connectors.models import Connector
from bamboo_lib.steps import LoadStep, DownloadStep

class ReadStep(PipelineStep):
    def run_step(self, prev, params):
        # income range data
        df = pd.read_excel(prev, dtype='str')

        df['id'] = df.id.astype(int)
        df['enigh_group_id'] = df['enigh_group_id'].astype(int)
        df['enoe_group_id'] = df['enoe_group_id'].astype(int)

        df = df[['id', 'enigh_group_id', 'enigh_group', 'enoe_group_id', 'enoe_group', 'name']].copy()

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
                'enigh_group_id':   'UInt8',
                'enigh_group':      'String',
                'enoe_group_id':    'UInt8',
                'enoe_group':       'String',
                'name':             'String'
            }

            download_step = DownloadStep(
            connector="dim-income",
            connector_path="conns.yaml"
            )

            read_step = ReadStep()
    
            load_step = LoadStep(
                'dim_shared_money', db_connector, if_exists='drop', pk=['id'], dtype=dtype
            )
    
            return [download_step, read_step, load_step]