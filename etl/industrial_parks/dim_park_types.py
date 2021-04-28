import pandas as pd

from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import Parameter, EasyPipeline, PipelineStep
from bamboo_lib.steps import DownloadStep, LoadStep


class TransformStep(PipelineStep):
    def run_step(self, prev, params):

        df = pd.read_csv(prev, sep=";", encoding="utf-8")
        df = df[["park_type_id", "type_park_es", "type_park_en"]]
        return df


class DimParkTypes(EasyPipeline):
    @staticmethod
    def steps(params):
        db_connector = Connector.fetch("clickhouse-database", open("../conns.yaml"))

        dtype = {
            "park_type_id":     "UInt8",
            "type_park_es":     "String",
            "type_park_en":     "String",
        }

        download_step = DownloadStep(
            connector="types_industrial_parks",
            connector_path="conns.yaml"
        )

        transform_step = TransformStep()

        load_step = LoadStep(
            "dim_types_industrial_parks", db_connector, dtype=dtype,
            if_exists="append", pk=["park_type_id"]
        )

        return [download_step, transform_step, load_step]


if __name__ == "__main__":
    dim_park_types_pipeline = DimParkTypes()
    dim_park_types_pipeline.run({})