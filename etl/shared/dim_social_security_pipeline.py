import pandas as pd
from bamboo_lib.models import PipelineStep
from bamboo_lib.models import EasyPipeline
from bamboo_lib.connectors.models import Connector
from bamboo_lib.steps import LoadStep, DownloadStep
# English stopwords
from sklearn.feature_extraction import stop_words

class ReadStep(PipelineStep):
    def run_step(self, prev, params):
        df = pd.read_excel(prev, sheet_name="social_security", encoding="latin-1")
        return df

class SocialSecurityPipeline(EasyPipeline):
    @staticmethod
    def description():
        return "Processes social security options from Mexico"

    @staticmethod
    def website():
        return "http://datawheel.us"

    @staticmethod
    def steps(params, **kwargs):
        # Use of connectors specified in the conns.yaml file
        db_connector = Connector.fetch("clickhouse-database", open("../conns.yaml"))
        dtype = {
            "id":                              "UInt8",
            "social_security_es":              "String",
            "social_security_en":              "String",
        }

        # Definition of each step
        download_step = DownloadStep(
            connector="social-security-academic-degree",
            connector_path="conns.yaml"
        )
        read_step = ReadStep()
        load_step = LoadStep("dim_shared_social_security", db_connector, if_exists="drop", pk=["id"], dtype=dtype)

        return [download_step, read_step, load_step]