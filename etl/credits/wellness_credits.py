
import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import Parameter, EasyPipeline, PipelineStep
from bamboo_lib.steps import DownloadStep, LoadStep
from shared import COLUMNS, AGE_RANGE, PERSON_TYPE, SEX, MISSING_MUN, replace_geo, norm, ReadStep

class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        df, credits_weeks_csv = prev

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

        df.loc[~df['mun_id'].isin(list(mun.values())), 'mun_id'] = \
            df.loc[~df['mun_id'].isin(list(mun.values())), 'ent_id'].astype(str) + '999'
        df.loc[df['level'] == 'Municipality', 'ent_id'] = '0'

        df = df[['ent_id', 'mun_id', 'level', 'sex', 'person_type', 'age_range', 'approved_week', 'count']].copy()

        for col in [x for x in df.columns if x not in ['level', 'approved_week']]:
            df[col] = df[col].astype(int)

        credits_weeks = pd.read_csv(credits_weeks_csv)
        df['approved_week'].replace(dict(zip(credits_weeks['approved_week_es'], credits_weeks['approved_week'])), inplace=True)

        return df

class WellnessWeeklyPipeline(EasyPipeline):
    @staticmethod
    def steps(params):
        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))

        dtype = {
            'ent_id':        'UInt8',
            'mun_id':        'UInt16',
            'level':         'String',
            'sex':           'UInt8',
            'person_type':   'UInt8',
            'age_range':     'UInt8',
            'approved_week': 'UInt32',
            'count':         'UInt64'
        }

        download_step = DownloadStep(
            connector=['wellness-weekly-ent', 'wellness-weekly-mun', 'dim-weeks'],
            connector_path='conns.yaml',
            force=True
        )

        read_step = ReadStep()
        transform_step = TransformStep()

        load_step = LoadStep(
            'wellness_weekly_credits', db_connector, dtype=dtype, if_exists='drop',
            pk=['ent_id', 'mun_id', 'level']
        )

        return [download_step, read_step, transform_step, load_step]

if __name__ == "__main__":
    pp = WellnessWeeklyPipeline()
    pp.run({})