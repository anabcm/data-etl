
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

        t = pd.DataFrame()
        for ele in data:
            df = pd.read_csv(ele, engine='python', sep="[\t]*,[\t]*", header=0)

            df = df.reset_index()

            # column names
            labels = pd.read_csv(label)
            columns = list(labels.reset_index()['index'])
            df.drop(columns=['ENTIDAD'], inplace=True)
            df.columns = columns
            df.replace(' ', np.nan, inplace=True)

            df = df.loc[~df['ID_ESTRATO'].isna()].copy()
            df = df.loc[~df['CODIGO'].isna()].copy()
            df = df.loc[~df['MUNICIPIO'].isna()].copy()
            df = df.loc[(df['CODIGO'].str.strip().str.len() == 2) | (df['CODIGO'] == '31-33') | (df['CODIGO'].str.strip() == '48-49')].copy()
            df = df[['ENTIDAD', 'MUNICIPIO', 'CODIGO', 'ID_ESTRATO','UE']].copy()
            df.columns = ['ent_id', 'mun_id', 'sector_id', 'estrato_id', 'ue']
            df['mun_id'] = df['ent_id'].astype(str).str.zfill(2) + df['mun_id'].astype(str).str.zfill(3)
            df = df[['mun_id', 'sector_id', 'estrato_id', 'ue']].copy()
            t = t.append(df)

        df = t.copy()

        for col in ['mun_id', 'ue', 'estrato_id']:
            df[col] = df[col].astype(int)

        return df

class EconomicCensusPipeline(EasyPipeline):
    @staticmethod
    def steps(params, **kwargs):
        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))

        dtypes = {
            'mun_id':           'UInt16',
            'sector_id':        'String',
            'estrato_id':       'UInt8',
            'ue':               'Float32'
        }

        download_step = WildcardDownloadStep(
            connector='economic-census-mun',
            connector_path="conns.yaml"
        )

        transform_step = TransformStep()
        load_step = LoadStep(
            'inegi_economic_census_mun_ue', db_connector, dtype=dtypes, if_exists='drop', 
            pk=['mun_id', 'sector_id', 'estrato_id']
        )

        return [download_step, transform_step, load_step]

if __name__ == '__main__':
    pp = EconomicCensusPipeline()
    pp.run({})