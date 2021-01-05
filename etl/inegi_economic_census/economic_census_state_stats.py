
import numpy as np
import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, PipelineStep, Parameter
from bamboo_lib.steps import LoadStep, WildcardDownloadStep
from bamboo_lib.helpers import grab_connector

class TransformStep(PipelineStep):
    def run_step(self, prev, params):

        # find data files
        data = [x[0] for x in prev if ('ce2019' in x[1]['file']) & ('diccionario' not in x[1]['file']) & ('national' not in x[1]['file'])]

        # find column names file
        label = [x[0] for x in prev if 'diccionario' in x[1]['file']][0]

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

            df = df.loc[(df['CODIGO'].str.strip().str.len() == 2) | (df['CODIGO'] == '31-33') | (df['CODIGO'].str.strip() == '48-49')].copy()

            df = df.loc[df['ID_ESTRATO'].isna()].copy()
            df = df.loc[df['MUNICIPIO'].isna()].copy()
            df = df.loc[~df['CODIGO'].isna()].copy()
            temp = temp.append(df)
            print(temp.shape, df.shape)

        df = temp.copy()
        df.drop(columns=['MUNICIPIO', 'ID_ESTRATO'], inplace=True)

        df.rename(columns={
            'ENTIDAD': 'ent_id',
            'UE': 'ue',
            'CODIGO': 'sector_id'
        }, inplace=True)

        for col in [x for x in df.columns if x != 'sector_id']:
            df[col] = df[col].astype(float)
        
        df['sector_id'] = df['sector_id'].astype(str)

        df['year'] = 2019

        df.columns = df.columns.str.lower()

        df = df[['ent_id', 'ue', 'year', 'sector_id'] + ['a111a', 'a121a', 'a131a', 'a211a', 'a221a', 'a511a', 
                'a700a', 'a800a', 'h000a', 'h000b', 'h000c', 'h000d', 'h001a', 'h001b', 'h001c', 
                'h001d', 'h010a', 'h010b', 'h010c', 'h010d', 'h020a', 'h020b', 'h020c', 'h020d', 
                'h101a', 'h101b', 'h101c', 'h101d', 'h203a', 'h203b', 'h203c', 'h203d', 'i000a', 
                'i000b', 'i000c', 'i000d', 'i100a', 'i100b', 'i100c', 'i100d', 'i200a', 'i200b', 
                'i200c', 'i200d', 'j000a', 'j010a', 'j203a', 'j300a', 'j400a', 'j500a', 'j600a', 
                'k000a', 'k010a', 'k020a', 'k030a', 'k040a', 'k050a', 'k060a', 'k070a', 'k090a', 
                'k096a', 'k311a', 'k610a', 'k620a', 'k810a', 'k820a', 'k910a', 'k950a', 'k976a', 
                'm000a', 'm010a', 'm020a', 'm030a', 'm050a', 'm090a', 'm091a', 'm700a', 'o010a', 
                'o020a', 'p000a', 'p000b', 'p000c', 'p030a', 'p030b', 'p030c', 'p100a', 'p100b', 
                'q000a', 'q000b', 'q000c', 'q000d', 'q010a', 'q020a', 'q030a', 'q400a', 'q900a']].copy()

        return df

class EconomicCensusPipeline(EasyPipeline):
    @staticmethod
    def steps(params, **kwargs):
        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))

        dtypes = {
            'ent_id':    'UInt8',
            'ue':        'Float32',
            'sector_id': 'String',
            'year':      'UInt16'
        }

        download_step = WildcardDownloadStep(
            connector='economic-census-mun',
            connector_path="conns.yaml"
        )

        transform_step = TransformStep()
        load_step = LoadStep(
            'inegi_economic_census_state_stats', db_connector, dtype=dtypes, if_exists='drop', 
            pk=['ent_id', 'year']
        )

        return [download_step, transform_step, load_step]

if __name__ == '__main__':
    pp = EconomicCensusPipeline()
    pp.run({})