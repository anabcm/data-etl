import pandas as pd
from bamboo_lib.models import PipelineStep
from bamboo_lib.models import EasyPipeline
from bamboo_lib.connectors.models import Connector
from bamboo_lib.steps import DownloadStep
from bamboo_lib.steps import LoadStep
from bamboo_lib.helpers import grab_connector

class ReadStep(PipelineStep):
    def run_step(self, prev, params):
        df = pd.read_csv(prev, dtype='str')
        return df

class CleanStep(PipelineStep):
    def run_step(self, prev, params):
        df = prev
        df = df.loc[:, ['sector_id', 'sector_es', 'sector_en']].drop_duplicates().copy()
        print(df.head())
        return df

class GDPNamesPipeline(EasyPipeline):
    @staticmethod
    def steps(params):
        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))
        dtype = {
            'sector_id':            'String',
            'sector_es':            'String',
            'sector_en':            'String',
        }

        download_step = DownloadStep(
            connector='naics-scian-codes',
            connector_path='conns.yaml'
        )

        read_step = ReadStep()
        clean_step = CleanStep()

        load_step = LoadStep('inegi_gdp_names', db_connector, if_exists='drop', pk=['sector_id'], dtype=dtype)

        return [download_step, read_step, clean_step, load_step]

if __name__ == "__main__":
    pp = GDPNamesPipeline()
    pp.run({})