import pandas as pd

from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import Parameter, EasyPipeline, PipelineStep
from bamboo_lib.steps import DownloadStep, LoadStep


class TransformStep(PipelineStep):
    def run_step(self, prev, params):

        df = pd.read_csv(prev, sep=";", encoding="utf-8")
        df = df[["indicator_id", "indicator_name_es", "indicator_name_en"]]
        return df


class DimIndicators(EasyPipeline):
    @staticmethod
    def steps(params):
        db_connector = Connector.fetch("clickhouse-database", open("../conns.yaml"))

        dtype = {
            "indicator_id":         "UInt8",
            "indicator_name_es":    "String",
            "indicator_name_en":    "String",
        }

        download_step = DownloadStep(
            connector="dim_indicators",
            connector_path="conns.yaml"
        )

        transform_step = TransformStep()

        load_step = LoadStep(
            "dim_indicators", db_connector, dtype=dtype,
            if_exists="append", pk=["indicator_id"]
        )

        return [download_step, transform_step, load_step]


if __name__ == "__main__":
    dim_indicators_pipeline = DimIndicators()
    dim_indicators_pipeline.run({})
