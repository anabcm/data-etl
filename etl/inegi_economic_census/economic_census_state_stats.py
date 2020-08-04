
import numpy as np
import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, PipelineStep, Parameter
from bamboo_lib.steps import LoadStep, WildcardDownloadStep
from bamboo_lib.helpers import grab_connector

class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        label = ''
        data = []
        for ele in prev:
            if 'diccionario' in ele[1]['file']:
                label = ele[0]
            elif 'ce2019_nac' in ele[1]['file']:
                continue
            else:
                data.append(ele[0])

        temp = pd.DataFrame()
        for ele in data:
            df = pd.read_csv(ele, engine='python', sep="[\t]*,[\t]*", header=0)

            df = df.reset_index()

            # column names
            labels = pd.read_csv(label)
            columns = list(labels.reset_index()['index'])
            df.drop(columns=['ENTIDAD'], inplace=True)
            df.columns = columns
            df.replace(' ', np.nan, inplace=True)

            df = df.loc[df['ID_ESTRATO'].isna()].copy()
            df = df.loc[df['CODIGO'].isna()].copy()
            df = df.loc[df['MUNICIPIO'].isna()].copy()
            temp = temp.append(df)
        
        df = temp.copy()
        df.drop(columns=['MUNICIPIO', 'CODIGO', 'ID_ESTRATO'], inplace=True)

        for col in list(df.columns[1::]):
            df[col] = df[col].astype(float)

        df.rename(columns={
            'ENTIDAD': 'ent_id',
            'UE': 'ue'
        }, inplace=True)

        df['year'] = 2019

        return df

class EconomicCensusPipeline(EasyPipeline):
    @staticmethod
    def steps(params, **kwargs):
        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))

        dtypes = {
            'ent_id':  'UInt8',
            'year':    'UInt16'
        }

        download_step = WildcardDownloadStep(
            connector='economic-census-mun',
            connector_path="conns.yaml"
        )

        # Definition of each step
        transform_step = TransformStep()
        load_step = LoadStep(
            'inegi_economic_census_state_stats', db_connector, dtype=dtypes, if_exists='drop', 
            pk=['ent_id', 'year']
        )

        return [download_step, transform_step, load_step]

if __name__ == '__main__':
    pp = EconomicCensusPipeline()
    pp.run({})