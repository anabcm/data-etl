
import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import Parameter, EasyPipeline, PipelineStep
from bamboo_lib.steps import DownloadStep, LoadStep
from shared import COLUMNS, AGE_RANGE, PERSON_TYPE, SEX, replace_geo, norm

class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        df = pd.read_csv(prev, encoding='latin-1')
        df.columns = df.columns.str.lower()
        df.rename(columns=COLUMNS, inplace=True)

        # filter confidential values
        df = df.loc[df['count'] != 'C'].copy()

        # replace members in dimensions
        df['sex'].replace(SEX, inplace=True)
        df['person_type'].replace(PERSON_TYPE, inplace=True)
        df['age_range'].replace(AGE_RANGE, inplace=True)
        df.drop(columns=['company_size'], inplace=True)

        df['ent_id'] = df['ent_id'].apply(lambda x: norm(x)).str.upper()

        # replace ent
        df['ent_id'].replace({'MEXICO': 15}, inplace=True)

        # replace names for ids
        ent, _mun = replace_geo()
        df['ent_id'] = df['ent_id'].replace(ent)

        df = df[['ent_id', 'sex', 'person_type', 'age_range', 'count']].copy()

        for col in df.columns:
            df[col] = df[col].astype(int)

        return df

class WellnessPipeline(EasyPipeline):
    @staticmethod
    def steps(params):
        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))

        dtype = {
            'ent_id':       'UInt8', 
            'sex':          'UInt8', 
            'person_type':  'UInt8', 
            'age_range':    'UInt8', 
            'count':        'UInt32'
        }

        download_step = DownloadStep(
            connector='wellness-ent-total',
            connector_path='conns.yaml',
            force=True
        )

        transform_step = TransformStep()

        load_step = LoadStep(
            'wellness_credits', db_connector, dtype=dtype, if_exists='drop',
            pk=['ent_id']
        )

        return [download_step, transform_step, load_step]

if __name__ == "__main__":
    pp = WellnessPipeline()
    pp.run({})