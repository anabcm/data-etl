import pandas as pd

from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import Parameter, EasyPipeline, PipelineStep
from bamboo_lib.steps import DownloadStep, LoadStep


class TransformStep(PipelineStep):
    def run_step(self, prev, params):

        df = pd.read_csv(prev, sep=";", encoding="utf-8")
        df = df[["park_id", "park_name"]]
        return df


class DimParkNames(EasyPipeline):
    @staticmethod
    def steps(params):
        db_connector = Connector.fetch("clickhouse-database", open("../conns.yaml"))

        dtype = {
            "park_id":      "UInt16",
            "park_name":    "String",
        }

        download_step = DownloadStep(
            connector="names_industrial_parks",
            connector_path="conns.yaml"
        )

        transform_step = TransformStep()

        load_step = LoadStep(
            "names_industrial_parks", db_connector, dtype=dtype,
            if_exists="append", pk=["park_id"]
        )

        return [download_step, transform_step, load_step]


if __name__ == "__main__":
    dim_park_names_pipeline = DimParkNames()
    dim_park_names_pipeline.run({})