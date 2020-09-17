import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import DownloadStep
from bamboo_lib.steps import LoadStep
from helpers import norm
from shared import SECTOR_REPLACE


class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        data = prev
        df = pd.read_excel(data, sheet_name=params.get('sheet_name'))
        df.columns = [norm(x.strip().lower().replace(' ', '_').replace('-', '_').replace('%', 'perc')) for x in df.columns]
        df = df.loc[~df[params.get('level')].str.contains('Total')].copy()

        split = df[params.get('level')].str.split(' ', n=1, expand=True)
        df[params.get('level')] = split[0]

        df.columns = list(params.get('dtype').keys())

        if params.get('pk') == 'sector_id':
            df['sector_id'].replace(SECTOR_REPLACE, inplace=True)

        else:
            df[params.get('pk')] = df[params.get('pk')].astype(int)

        df['value'] = 0
        df = df.loc[df['value_c'].astype(str).str.lower() != 'c'].copy()
        df['value_c'] = df['value_c'].astype(float)

        return df

class FDI7Pipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return [
            Parameter(name="pk", dtype=str),
            Parameter(name="sheet_name", dtype=str),
            Parameter(name="level", dtype=str),
            Parameter(name="dtype", dtype=str),
            Parameter(name="table", dtype=str)
        ]

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch("clickhouse-database", open("../conns.yaml"))

        download_step = DownloadStep(
            connector="fdi-data",
            connector_path="conns.yaml"
        )
        transform_step = TransformStep()
        load_step = LoadStep(
            params.get('table'), db_connector, if_exists="drop", 
            pk=[params.get('pk')], dtype=params.get('dtype')
        )

        return [download_step, transform_step, load_step]