import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, PipelineStep, Parameter
from bamboo_lib.steps import DownloadStep, LoadStep

class TransformStep(PipelineStep):
    def run_step(self, prev, params):

        df = pd.read_csv(prev)
        df.columns = df.columns.str.lower()
        print("suma: ", df.factor.sum())

        return df

class PeoplePipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return [
            Parameter(label="Index", name="index", dtype=str)
        ]

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch("clickhouse-database", open("../conns.yaml"))

        dtype = {
        }

        download_step = DownloadStep(
            connector="people-data",
            connector_path="conns.yaml"
        )
        transform_step = TransformStep()

        return [download_step, transform_step]