import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import DownloadStep
from bamboo_lib.steps import LoadStep
from helpers import norm
from shared import get_dimensions, COUNTRY_REPLACE


class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        data = prev
        df = pd.read_excel(data, sheet_name='3')
        df.columns = [norm(x.strip().lower().replace(' ', '_').replace('-', '_').replace('%', 'perc')) for x in df.columns]
        df = df.loc[~df['entidad_federativa'].str.contains('Total')].copy()

        # get country, end_id dimensions
        dim_geo, dim_country = get_dimensions()

        df['entidad_federativa'].replace(dict(zip(dim_geo['ent_name'], dim_geo['ent_id'])), inplace=True)

        df['pais'].replace(COUNTRY_REPLACE, inplace=True)
        df['pais'] = df['pais'].replace(dict(zip(dim_country['country_name_es'], dim_country['iso3'])))

        df.columns = ['ent_id', 'year', 'country', 'value', 'count', 'value_c']
        df.drop(columns=['value'], inplace=True)
        df = df.loc[df['value_c'].astype(str).str.lower() != 'c'].copy()
        df['value_c'] = df['value_c'].astype(float)

        return df

class FDI3Pipeline(EasyPipeline):
    @staticmethod
    def steps(params):
        db_connector = Connector.fetch("clickhouse-database", open("../conns.yaml"))

        dtype = {
            'ent_id':  'UInt8',
            'year':    'UInt16',
            'country': 'String',
            'count':   'UInt16',
            'value_c': 'Float32'
        }

        download_step = DownloadStep(
            connector="fdi-data",
            connector_path="conns.yaml",
            force=True
        )

        transform_step = TransformStep()
        load_step = LoadStep(
            'fdi_3', db_connector, if_exists="drop", 
            pk=['ent_id', 'country'], dtype=dtype
        )

        return [download_step, transform_step, load_step]

if __name__ == "__main__":
    pp = FDI3Pipeline()
    pp.run({})