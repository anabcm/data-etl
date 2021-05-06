import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import Parameter, EasyPipeline, PipelineStep
from bamboo_lib.steps import DownloadStep, LoadStep
from shared import AGE_RANGE, PERSON_TYPE, SEX, MISSING_MUN, replace_geo, norm
from helpers import norm

COLUMNS = {
    'clave municipal': 'clave',
    'nom ent': 'ent_id',
    'nom mun': 'mun_id',
    'sexo': 'sex',
    'tipo_persona': 'person_type',
    'rango_edad_antigüedad': 'age_range',
    'conteo_anonimizado': 'count'
}

class ReadStep(PipelineStep):
    def run_step(self, prev, params):
        try:
            df = pd.read_csv(prev, encoding='latin-1')
        except pd.errors.ParserError:
            df = pd.read_excel(prev)

        df.columns = df.columns.str.lower()
        df.rename(columns=COLUMNS, inplace=True)
        df['level'] = 'Municipality'

        return df

class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        df = prev

        # replace members in dimensions
        df['sex'].replace(SEX, inplace=True)
        df['person_type'] = df['person_type'].apply(lambda x: norm(x)).str.lower()
        df['person_type'].replace(PERSON_TYPE, inplace=True)
        df['age_range'].replace(AGE_RANGE, inplace=True)
        
        for col in ['ent_id', 'mun_id']:
            df[col] = df[col].apply(lambda x: norm(x)).str.upper()

        # replace missing municipalities
        df['mun_id'].replace(MISSING_MUN, inplace=True)

        # replace ent
        df['ent_id'].replace({'MEXICO': 15}, inplace=True)

        # replace names for ids
        ent, mun = replace_geo()
        df['ent_id'] = df['ent_id'].replace(ent)
        df['mun_id'] = df['mun_id'].replace(mun)

        df.loc[~df['mun_id'].isin(list(mun.values())), 'mun_id'] = \
            df.loc[~df['mun_id'].isin(list(mun.values())), 'ent_id'].astype(str) + '999'

        df = df[['mun_id', 'level', 'sex', 'person_type', 'age_range', 'count']].copy()

        for col in df.columns[df.columns != 'level']:
            df[col] = df[col].astype(int)

        return df

class FemalesPipeline(EasyPipeline):
    @staticmethod
    def steps(params):
        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))

        dtype = { 
            'mun_id':           'UInt16', 
            'level':            'String', 
            'sex':              'UInt8', 
            'person_type':      'UInt8', 
            'age_range':        'UInt8',
            'count':            'UInt32'
        }

        download_step = DownloadStep(
            connector='females-mun-total',
            connector_path='conns.yaml',
            force=True
        )

        read_step = ReadStep()
        transform_step = TransformStep()

        load_step = LoadStep(
            'females_credits', db_connector, dtype=dtype, if_exists='drop',
            pk=['mun_id', 'level']
        )

        return [download_step, read_step, transform_step, load_step]

if __name__ == "__main__":
    pp = FemalesPipeline()
    pp.run({})