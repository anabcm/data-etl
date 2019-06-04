import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import DownloadStep
from bamboo_lib.steps import LoadStep


class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        df = pd.read_csv(prev, index_col=None, header=0, encoding='latin-1')
        df.columns = df.columns.str.lower()

        primary_cols = ['ent', 'nom_ent', 'mun', 'nom_mun']

        # Condense municipality and state IDs into a single column
        final_df = df[primary_cols]
        final_df['geo_id'] = final_df['ent'].astype(str) + final_df['mun'].astype(str).str.zfill(3)
        final_df['geo_id'] = final_df['geo_id'].astype(int)

        final_df.rename(columns={
            'ent': 'state_id',
            'nom_ent': 'state',
            'mun': 'municipality_id',
            'nom_mun': 'municipality',
        }, inplace=True)

        final_df.drop_duplicates(inplace=True)

        return final_df


class DimGeographyPipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return [
            Parameter(label="Index", name="index", dtype=str),
        ]

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch('clickhouse-database', open("etl/conns.yaml"))

        dtype = {
            'geo_id':            'UInt32',
            'state_id':          'UInt8',
            'state':             'String',
            'municipality_id':   'UInt16',
            'municipality':      'String',
        }

        download_step = DownloadStep(
            connector='population-data',
            connector_path='etl/inegi_intercensal_census/conns.yaml'
        )
        transform_step = TransformStep()
        load_step = LoadStep(
            "dim_inegi_geography", db_connector, if_exists="append", dtype=dtype,
            pk=['geo_id', 'state_id', 'municipality_id']
        )

        return [download_step, transform_step, load_step]
