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
        df = pd.read_excel(df_labels, 'ent_pais_res_5a')
        df = df.drop(columns=['prev_id'])
        return df


class DimEntityCountryNationPipeline(EasyPipeline):
    @staticmethod
    def steps(params):
        db_connector = Connector.fetch('clickhouse-database', open("../conns.yaml"))

        dtype = {
            'id':       'UInt16',
            'name_es':  'String',
            'name_en':  'String',
        }
        download_step = DownloadStep(
            connector='labels-2',
            connector_path='conns.yaml'
        )
        extract_step = ExtractStep()
        load_step = LoadStep(
            "dim_inegi_entity_country_nation", db_connector, if_exists="drop", dtype=dtype,
            pk=['id']
        )

        return [download_step, extract_step, load_step]