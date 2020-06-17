
import pandas as pd
from bamboo_lib.helpers import query_to_df
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import DownloadStep
from bamboo_lib.steps import LoadStep

class ExtractStep(PipelineStep):
    def run_step(self, prev, params):
        df = pd.read_excel(prev, header=4)

        df.dropna(inplace=True)

        df.rename(columns={'Unnamed: 0': 'ent_id'}, inplace=True)

        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))
        states = query_to_df(db_connector, raw_query='select ent_name, ent_id from dim_shared_geography_ent')

        df['ent_id'].replace(dict(zip(states['ent_name'], states['ent_id'])), inplace=True)

        df = df.melt(id_vars='ent_id', var_name='decile')

        df.replace({'Estados Unidos Mexicanos': 33}, inplace=True)

        df['value'] = df['value'].astype(int)

        return df

class WagePipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return [
            Parameter(label='Year', name='year', dtype=str)
        ]

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))
        dtype = {
            'ent_id':   'UInt8',
            'decile':   'String',
            'value':    'Float32'
        }

        download_step = DownloadStep(
            connector='enigh-household-income',
            connector_path='conns.yaml'
        )
        extract_step = ExtractStep()
        load_step = LoadStep(
            'dim_enigh_income', db_connector, if_exists='append', dtype=dtype,
            pk=['ent_id']
        )

        return [download_step, extract_step, load_step]

if __name__ == '__main__':
    for year in [2016, 2018]:
        pp = WagePipeline()
        pp.run({'year': year})