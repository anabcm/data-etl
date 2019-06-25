import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import DownloadStep
from bamboo_lib.steps import LoadStep

class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        df = pd.read_csv("https://docs.google.com/spreadsheets/d/e/2PACX-1vQ_cYou4TSCU8zxH3S6Ze0HZM8Dv3bBUSXKYVN5UYMnkinSwjnAuSHr-T-mmLjALMFxyH0JduBECM6F/pub?output=csv")
        return df

class DimCountryGeographyPipeline(EasyPipeline):
    @staticmethod
    def steps(params):
        db_connector = Connector.fetch("clickhouse-database", open("../conns.yaml"))

        transform_step = TransformStep()
        load_step = LoadStep(
            "dim_shared_country", db_connector, if_exists="drop", nullable_list=["iso2"],
            pk=["iso3"]
        )

        return [transform_step, load_step]