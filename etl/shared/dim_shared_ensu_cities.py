import pandas as pd
from bamboo_lib.models import PipelineStep
from bamboo_lib.models import EasyPipeline
from bamboo_lib.connectors.models import Connector
from bamboo_lib.steps import LoadStep

class ReadStep(PipelineStep):
    def run_step(self, prev, params):
        url = "https://docs.google.com/spreadsheets/d/e/2PACX-1vQSnOLOLTXeKjlrLM-SRqEKC0v4ATLeAIEwYA4clBWmu3klCRnkcREwJrCVvCO2WDPzzpTPP1Ocj-H0/pub?output=xlsx"
        df = pd.read_excel(url, sheet_name="dim_city", encoding="latin-1")
        return df

class CoveragePipeline(EasyPipeline):
    @staticmethod
    def description():
        return "Processes the cities in the ENSU survey from Mexico"

    @staticmethod
    def website():
        return "http://datawheel.us"

    @staticmethod
    def steps(params, **kwargs):
        # Use of connectors specified in the conns.yaml file
        db_connector = Connector.fetch("clickhouse-database", open("../conns.yaml"))
        dtype = {
            "reference_city_id":                         "UInt8",
            "reference_city_name":                       "String",
        }

        # Definition ofreference_city_ each step
        read_step = ReadStep()
        load_step = LoadStep("dim_shared_ensu_cities", db_connector, if_exists="drop", pk=["reference_city_id"], dtype=dtype)

        return [read_step, load_step]