import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import DownloadStep
from bamboo_lib.steps import LoadStep


class ExtractStep(PipelineStep):
    def run_step(self, prev, params):
        return pd.read_csv("https://docs.google.com/spreadsheets/d/e/2PACX-1vTboGfNlyt6zp6BeNLIkU01YJyZa2M4J0_OegPUtB1mraxdlgKmfTB6JO58VElWKKeXCV5J2-Exv9kz/pub?output=csv")

class DimSexPipeline(EasyPipeline):
    @staticmethod
    def steps(params):
        db_connector = Connector.fetch("clickhouse-database", open("../conns.yaml"))
        dtype = {
            "time":         "UInt8",
            "year":         "UInt16",
            "quarter":      "String"
        }

        extract_step = ExtractStep()
        load_step = LoadStep(
            "dim_shared_time_enoe", db_connector, if_exists="drop", dtype=dtype, pk=["time"]
        )

        return [extract_step, load_step]

