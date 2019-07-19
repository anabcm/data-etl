import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import DownloadStep
from bamboo_lib.steps import LoadStep


class ExtractStep(PipelineStep):
    def run_step(self, prev, params):
        df_labels = pd.ExcelFile("https://docs.google.com/spreadsheets/d/e/2PACX-1vQUxKRAWCVSxUhl2S0fbP46C9JFnKYp6HjXAUX2-jup_iYKjnLnylWbxrf8NVKmL_raC4Vk_BgQT48G/pub?output=xlsx")
        df = pd.read_excel(df_labels, "age_range")
        return df

class DimSexPipeline(EasyPipeline):
    @staticmethod
    def steps(params):
        db_connector = Connector.fetch("clickhouse-database", open("etl/conns.yaml"))
        dtype = {
            "age":           "UInt8",
            "name_es":       "String",
            "name_en":       "String",
            "age_range_id":  "UInt8"
        }

        extract_step = ExtractStep()
        load_step = LoadStep(
            "dim_shared_sex", db_connector, if_exists="append", dtype=dtype,
            pk=["age"]
        )

        return [extract_step, load_step]

