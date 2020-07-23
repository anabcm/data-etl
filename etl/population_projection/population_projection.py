
import pandas as pd
from bamboo_lib.models import PipelineStep
from bamboo_lib.models import EasyPipeline
from bamboo_lib.connectors.models import Connector
from bamboo_lib.steps import DownloadStep
from bamboo_lib.steps import LoadStep
from bamboo_lib.helpers import grab_connector

class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        df_a = pd.read_csv(prev[0], encoding='latin-1')
        df_b = pd.read_csv(prev[1], encoding='latin-1')

        for df in [df_a, df_b]:
            df.columns = df.columns.str.lower()

        df = df_a.append(df_b)

        df = df[['clave', 'sexo', 'año', 'edad_quin', 'pob']].copy()
        df.columns = ['mun_id', 'sex', 'year', 'age', 'population']

        sex = {
            'Hombres': 1,
            'Mujeres': 2
        }

        df['sex'] = df['sex'].replace(sex)

        age_range = {
            'pobm_00_04': 1,
            'pobm_05_09': 2,
            'pobm_10_14': 3,
            'pobm_15_19': 4,
            'pobm_20_24': 5,
            'pobm_25_29': 6,
            'pobm_30_34': 7,
            'pobm_35_39': 8,
            'pobm_40_44': 9,
            'pobm_45_49': 10,
            'pobm_50_54': 11,
            'pobm_55_59': 12,
            'pobm_60_64': 13,
            'pobm_65_mm': 14
        }
        df['age'] = df['age'].replace(age_range)

        return df

class PopulationPipeline(EasyPipeline):
    @staticmethod
    def steps(params):

        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))

        dtype = {
            'mun_id':      'UInt16',
            'sex':         'UInt8',
            'year':        'UInt16',
            'age':         'UInt8',
            'population':  'UInt32'
        }

        download_step = DownloadStep(
            connector=['population-data-1', 'population-data-2'],
            connector_path="conns.yaml"
        )

        transform_step = TransformStep()

        load_step = LoadStep('population_projection', db_connector, if_exists='drop', pk=['mun_id'], dtype=dtype)

        return [download_step, transform_step, load_step]

if __name__ == "__main__":
    pp = PopulationPipeline()
    pp.run({})