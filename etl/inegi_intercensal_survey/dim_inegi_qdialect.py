import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import DownloadStep
from bamboo_lib.steps import LoadStep


class ExtractStep(PipelineStep):
    def run_step(self, prev, params):
        df_labels = pd.ExcelFile(prev)
        df = pd.read_excel(df_labels, 'qdialect_inali')
        df = df.drop(columns=['prev_id'])
        return df


class DimQdialectPipeline(EasyPipeline):
    @staticmethod
    def steps(params):
        db_connector = Connector.fetch('clickhouse-database', open("../conns.yaml"))

        dtype = {
            'id':       'UInt8',
            'name_es':  'String',
        }
        download_step = DownloadStep(
            connector='labels-2',
            connector_path='conns.yaml'
        )
        extract_step = ExtractStep()
        load_step = LoadStep(
            "dim_inegi_qdialect", db_connector, if_exists="drop", dtype=dtype,
            pk=['id']
        )

        return [download_step, extract_step, load_step]