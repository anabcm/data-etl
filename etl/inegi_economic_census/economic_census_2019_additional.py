
import numpy as np
import pandas as pd
from bamboo_lib.logger import logger
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, PipelineStep, Parameter
from bamboo_lib.steps import WildcardDownloadStep, LoadStep
from static import FILL_COLUMNS


def fill_level(df, columns):
    for col in columns:
        if col not in df.columns:
            df[col] = 0
    return df


class ReadStep(PipelineStep):
    def run_step(self, prev, params):
        logger.info('Read Step...')

        label = ''
        data = []
        for ele in prev:
            if 'diccionario' in ele[1]['file']:
                label = ele[0]
            elif 'ce2019_nac' in ele[1]['file']:
                continue
            elif 'Indicators' in ele[1]['file']:
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
            t = t.append(df)

        df = t.copy()
        t = pd.DataFrame()
        
        df['year'] = 2019

        return df

class EntSec(PipelineStep):
    def run_step(self, prev, params):
        logger.info('State - Sector Step...')
        base = prev

        df = base.copy()

        df = df.loc[(df['CODIGO'].str.strip().str.len() == 2) | (df['CODIGO'] == '31-33') | (df['CODIGO'].str.strip() == '48-49')].copy()

        df = df.loc[df['ID_ESTRATO'].isna()].copy()
        df = df.loc[df['MUNICIPIO'].isna()].copy()
        df = df.loc[~df['CODIGO'].isna()].copy()

        df['sector_id'] = df['CODIGO'].astype(str).str.strip()
        df['ent_id'] = df['ENTIDAD'].astype(int)

        df.drop(columns=['ID_ESTRATO', 'CODIGO', 'MUNICIPIO', 'ENTIDAD'], inplace=True)

        df = fill_level(df, FILL_COLUMNS)

        # Entidad-Sector
        df['level'] = 1

        df_ent_sec = df.copy()

        return df_ent_sec, base

class EntSubsector(PipelineStep):
    def run_step(self, prev, params):
        logger.info('State - Subsector Step...')
        df_ent_sec, base = prev

        df = base.copy()

        df = df.loc[df['CODIGO'].str.strip().str.len() == 3].copy()

        df = df.loc[df['ID_ESTRATO'].isna()].copy()
        df = df.loc[df['MUNICIPIO'].isna()].copy()
        df = df.loc[~df['CODIGO'].isna()].copy()

        df['subsector_id'] = df['CODIGO'].astype(int)
        df['ent_id'] = df['ENTIDAD'].astype(int)

        df.drop(columns=['ID_ESTRATO', 'CODIGO', 'MUNICIPIO', 'ENTIDAD'], inplace=True)

        df = fill_level(df, FILL_COLUMNS)

        # Entidad-Subsector
        df['level'] = 2

        df_ent_sub = df.copy()

        return df_ent_sec, df_ent_sub, base

class EntRama(PipelineStep):
    def run_step(self, prev, params):
        logger.info('State - Industry Step...')
        df_ent_sec, df_ent_sub, base = prev

        df = base.copy()

        df = df.loc[df['CODIGO'].str.strip().str.len() == 4].copy()

        df = df.loc[df['ID_ESTRATO'].isna()].copy()
        df = df.loc[df['MUNICIPIO'].isna()].copy()
        df = df.loc[~df['CODIGO'].isna()].copy()

        df['rama_id'] = df['CODIGO'].astype(int)
        df['ent_id'] = df['ENTIDAD'].astype(int)

        df.drop(columns=['ID_ESTRATO', 'CODIGO', 'MUNICIPIO', 'ENTIDAD'], inplace=True)

        df = fill_level(df, FILL_COLUMNS)

        # Entidad-Rama
        df['level'] = 3

        df_ent_ram = df.copy()

        return df_ent_sec, df_ent_sub, df_ent_ram, base

class MunSec(PipelineStep):
    def run_step(self, prev, params):
        logger.info('Municipality - Sector Step...')
        df_ent_sec, df_ent_sub, df_ent_ram, base = prev

        df = base.copy()

        df = df.loc[(df['CODIGO'].str.strip().str.len() == 2) | (df['CODIGO'] == '31-33') | (df['CODIGO'].str.strip() == '48-49')].copy()

        df = df.loc[df['ID_ESTRATO'].isna()].copy()
        df = df.loc[~df['MUNICIPIO'].isna()].copy()
        df = df.loc[~df['CODIGO'].isna()].copy()

        df['sector_id'] = df['CODIGO'].astype(str).str.strip()
        df['mun_id'] = (df['ENTIDAD'].astype(str) + df['MUNICIPIO'].astype(str).str.zfill(3)).astype(int)

        df.drop(columns=['ID_ESTRATO', 'CODIGO', 'MUNICIPIO', 'ENTIDAD'], inplace=True)

        df = fill_level(df, FILL_COLUMNS)

        # Municipio-Sector
        df['level'] = 4

        df_mun_sec = df.copy()

        return df_ent_sec, df_ent_sub, df_ent_ram, df_mun_sec

class JoinStep(PipelineStep):
    def run_step(self, prev, params):
        logger.info('Join Step...')
        df_ent_sec, df_ent_sub, df_ent_ram, df_mun_sec = prev

        df = pd.DataFrame()
        for _df in [df_ent_sec, df_ent_sub, df_ent_ram, df_mun_sec]:
            df = df.append(_df, sort=False)

        df[list(df.columns[df.columns != 'sector_id'])] = df[list(df.columns[df.columns != 'sector_id'])].astype(float)
        df['sector_id'] = df['sector_id'].astype(str)
        df[['UE', 'level']] = df[['UE', 'level']].astype(int)

        df['year'] = 2019

        df.columns = df.columns.str.lower()

        return df

class EconomicCensusPipeline(EasyPipeline):
    @staticmethod
    def steps(params, **kwargs):
        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))

        dtypes = {
            'ent_id':           'UInt8',
            'mun_id':           'UInt16',
            'sector_id':        'String',
            'subsector_id':     'UInt16',
            'rama_id':          'UInt16',
            'ue':               'UInt32',
            'level':            'UInt8',
            'year':             'UInt16'
        }

        download_step = WildcardDownloadStep(
            connector='economic-census-mun',
            connector_path='conns.yaml'
        )

        read_step = ReadStep()
        ent_sector = EntSec()
        ent_subsector = EntSubsector()
        ent_rama = EntRama()
        mun_sec = MunSec()
        join_step = JoinStep()

        load_step = LoadStep(
            'inegi_economic_census_additional', db_connector, dtype=dtypes, if_exists='drop', 
            pk=['ent_id', 'mun_id', 'sector_id', 'subsector_id', 'rama_id', 'level', 'year'],
            nullable_list= ['a111a', 'a121a', 'a131a', 'a211a', 'a221a', 'a511a', 'a700a',
                            'a800a', 'h000a', 'h000b', 'h000c', 'h000d', 'h001a', 'h001b',
                            'h001c', 'h001d', 'h010a', 'h010b', 'h010c', 'h010d', 'h020a',
                            'h020b', 'h020c', 'h020d', 'h101a', 'h101b', 'h101c', 'h101d',
                            'h203a', 'h203b', 'h203c', 'h203d', 'i000a', 'i000b', 'i000c',
                            'i000d', 'i100a', 'i100b', 'i100c', 'i100d', 'i200a', 'i200b',
                            'i200c', 'i200d', 'j000a', 'j010a', 'j203a', 'j300a', 'j400a',
                            'j500a', 'j600a', 'k000a', 'k010a', 'k020a', 'k030a', 'k040a',
                            'k050a', 'k060a', 'k070a', 'k090a', 'k096a', 'k311a', 'k610a',
                            'k620a', 'k810a', 'k820a', 'k910a', 'k950a', 'k976a', 'm000a',
                            'm010a', 'm020a', 'm030a', 'm050a', 'm090a', 'm091a', 'm700a',
                            'o010a', 'o020a', 'p000a', 'p000b', 'p000c', 'p030a', 'p030b',
                            'p030c', 'p100a', 'p100b', 'q000a', 'q000b', 'q000c', 'q000d',
                            'q010a', 'q020a', 'q030a', 'q400a', 'q900a', 'a112a', 'a113a',
                            'a114a', 'a115a', 'a116a', 'a117a', 'a118a', 'a119a', 'a141a',
                            'a143a', 'a144a', 'a145a', 'a146a', 'a147a', 'a148a', 'a149a',
                            'a151a', 'a152a', 'a153a', 'a154a', 'a155a', 'a156a', 'a157a',
                            'a158a', 'a159a', 'a171a', 'a173a', 'a174a', 'a175a', 'a176a',
                            'a177a', 'a179a', 'a181a', 'a182a', 'a192a', 'a193a', 'a194a',
                            'a195a', 'a197a', 'a201a', 'a202a', 'a204a', 'a206a', 'a208a',
                            'a209a', 'a212a', 'a213a', 'a214a', 'a215a', 'a216a', 'a217a',
                            'a218a', 'a219a', 'a222a', 'a223a', 'a224a', 'a225a', 'a226a',
                            'a227a', 'a228a', 'a229a', 'a231a', 'a233a', 'a235a', 'a236a',
                            'a237a', 'a238a', 'a242a', 'a243a', 'a244a', 'a245a', 'a247a',
                            'a248a', 'a252a', 'a253a', 'a256a', 'a433a', 'a434a', 'a436a',
                            'a529a', 'a534a', 'a747a', 'a748a', 'a752a', 'a764a']
        )

        return [download_step, read_step, ent_sector, ent_subsector, ent_rama, mun_sec, join_step, load_step]

if __name__ == '__main__':
    pp = EconomicCensusPipeline()
    pp.run({})
