
import numpy as np
import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, PipelineStep, Parameter
from bamboo_lib.steps import DownloadStep, LoadStep
from bamboo_lib.helpers import grab_connector

class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        df = pd.read_csv(prev[0], engine='python', sep='[\t]*,[\t]*', header=0)
        df = df.reset_index()
        labels = pd.read_csv(prev[1])
        columns = list(labels.reset_index()['index'])
        df.drop(columns=['ENTIDAD'], inplace=True)
        df.columns = columns
        df.replace(' ', np.nan, inplace=True)
        df['nation_id'] = 'mex'
        df['year'] = 2019
        df = df.loc[(df['CODIGO'].str.strip().str.len() == 2) | (df['CODIGO'].str.strip() == '31-33') | (df['CODIGO'].str.strip() == '48-49')].copy()
        df = df.loc[df['ID_ESTRATO'].isna()].copy()
        df = df[['CODIGO'] + list(df.columns[4::])].copy()

        for col in list(df.columns[1:-2]):
            df[col] = df[col].astype(float)

        df.rename(columns={
            'CODIGO': 'sector_id',
            'UE': 'ue'
        }, inplace=True)

        return df

class EconomicCensusPipeline(EasyPipeline):
    @staticmethod
    def steps(params, **kwargs):
        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))

        dtypes = {
            'nation_id':               'String',
            'year':                    'UInt16'
        }

        download_step = DownloadStep(
            connector=['economic-census-nat', 'economic-census-columns'],
            connector_path='conns.yaml'
        )

        # Definition of each step
        transform_step = TransformStep()
        load_step = LoadStep(
            'inegi_economic_census_nat', db_connector, dtype=dtypes, if_exists='drop', 
            pk=['nation_id', 'year']
        )

        return [download_step, transform_step, load_step]

if __name__ == '__main__':
    pp = EconomicCensusPipeline()
    pp.run({})