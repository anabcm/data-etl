import pandas as pd
from bamboo_lib.models import PipelineStep
from bamboo_lib.models import EasyPipeline
from bamboo_lib.connectors.models import Connector
from bamboo_lib.steps import LoadStep, DownloadStep

class ReadStep(PipelineStep):
    def run_step(self, prev, params):
        df = pd.read_excel(prev, sheet_name="academic_degree", encoding="latin-1")
        return df

class CoveragePipeline(EasyPipeline):
    @staticmethod
    def description():
        return "Processes academic degrees from Mexico"

    @staticmethod
    def website():
        return "http://datawheel.us"

    @staticmethod
    def steps(params, **kwargs):
        # Use of connectors specified in the conns.yaml file
        db_connector = Connector.fetch("clickhouse-database", open("../conns.yaml"))
        dtype = {
            "id":                              "UInt8",
            "academic_degree_es":              "String",
            "academic_degree_en":              "String",
        }

        # Definition of each step
        download_step = DownloadStep(
            connector="social-security-academic-degree",
            connector_path="conns.yaml"
        )
        read_step = ReadStep()
        load_step = LoadStep("dim_shared_academic_degree", db_connector, if_exists="drop", pk=["id"], dtype=dtype)

        return [download_step, read_step, load_step]