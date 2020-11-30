import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import DownloadStep
from bamboo_lib.steps import LoadStep

class ExtractStep(PipelineStep):
    def run_step(self, prev, params):

        df = pd.read_excel(prev)

        return df

class DimEconomicSectorPipeline(EasyPipeline):
    @staticmethod
    def steps(params):
        db_connector = Connector.fetch("clickhouse-database", open("../conns.yaml"))
        dtype = {
            "level_1_id":   "UInt8",
            "level_1_name_es": "String",
            "level_1_name_en": "String",
            "level_2_id":   "UInt8",
            "level_2_name_es": "String",
            "level_2_name_en": "String",
            "level_4_id":   "UInt16",
            "level_4_name_es": "String",
            "level_4_name_en": "String"
        }

        download_step = DownloadStep(
            connector="imss-economic-sector-dimension",
            connector_path="conns.yaml"
        )

        extract_step = ExtractStep()
        load_step = LoadStep(
            "imss_economic_sector", db_connector, if_exists="drop", dtype=dtype,
            pk=["level_4_id", "level_2_id", "level_1_id"]
        )

        return [download_step, extract_step, load_step]

if __name__ == "__main__":
    pp = DimEconomicSectorPipeline()
    pp.run({})
