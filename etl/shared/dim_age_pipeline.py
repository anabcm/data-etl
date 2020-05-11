import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import DownloadStep
from bamboo_lib.steps import LoadStep


class ExtractStep(PipelineStep):
    def run_step(self, prev, params):
        df_labels = pd.ExcelFile("https://storage.googleapis.com/datamexico-data/shared/age_range.xlsx")
        df = pd.read_excel(df_labels, "age_range")
        return df

class DimSexPipeline(EasyPipeline):
    @staticmethod
    def steps(params):
        db_connector = Connector.fetch("clickhouse-database", open("../conns.yaml"))
        dtype = {
            "age":           "UInt8",
            "name_es":       "String",
            "name_en":       "String",
            "age_range_id":  "UInt8"
        }

        extract_step = ExtractStep()
        load_step = LoadStep(
            "dim_shared_age", db_connector, if_exists="drop", dtype=dtype,
            pk=["age"]
        )

        return [extract_step, load_step]
