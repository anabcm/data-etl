
import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import Parameter, EasyPipeline, PipelineStep
from bamboo_lib.steps import DownloadStep, LoadStep
from shared import COLUMNS, AGE_RANGE, PERSON_TYPE, SEX, MISSING_MUN, replace_geo, norm, ReadStep

class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        df = prev
        # filter confidential values
        df = df.loc[df['count'].astype(str).str.lower() != 'c'].copy()

        # replace members in dimensions
        df['person_type'] = df['person_type'].str.strip().str.lower().apply(lambda x: norm(x))
        df['sex'].replace(SEX, inplace=True)
        df['person_type'].replace(PERSON_TYPE, inplace=True)
        df['age_range'].replace(AGE_RANGE, inplace=True)
        df.drop(columns=['company_size'], inplace=True)

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

        df = df[['ent_id', 'mun_id', 'sex', 'person_type', 'age_range', 'count', 'level']].copy()

        for col in df.columns[df.columns != 'level']:
            df[col] = df[col].astype(int)

        return df

class WellnessPipeline(EasyPipeline):
    @staticmethod
    def steps(params):
        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))

        dtype = {
            'ent_id':       'UInt8',
            'mun_id':       'UInt16',
            'sex':          'UInt8', 
            'level':        'String',
            'person_type':  'UInt8', 
            'age_range':    'UInt8', 
            'count':        'UInt32'
        }

        download_step = DownloadStep(
            connector=['wellness-ent-total', 'wellness-mun-total'],
            connector_path='conns.yaml',
            force=True
        )

        read_step = ReadStep()

        transform_step = TransformStep()

        load_step = LoadStep(
            'wellness_credits', db_connector, dtype=dtype, if_exists='drop',
            pk=['ent_id']
        )

        return [download_step, read_step, transform_step, load_step]

if __name__ == "__main__":
    pp = WellnessPipeline()
    pp.run({})