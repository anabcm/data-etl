import pandas as pd

from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import Parameter, EasyPipeline, PipelineStep
from bamboo_lib.steps import DownloadStep, LoadStep


class TransformStep(PipelineStep):
    def run_step(self, prev, params):

        df = pd.read_csv(prev, sep=";", encoding="utf-8")
        df = df[["category_id", "category_name_es", "category_name_en"]]
        return df


class DimCategories(EasyPipeline):
    @staticmethod
    def steps(params):
        db_connector = Connector.fetch("clickhouse-database", open("../conns.yaml"))

        dtype = {
            "category_id":          "UInt8",
            "category_name_es":     "String",
            "category_name_en":     "String",
        }

        download_step = DownloadStep(
            connector="dim_categories",
            connector_path="conns.yaml"
        )

        transform_step = TransformStep()

        load_step = LoadStep(
            "dim_categories", db_connector, dtype=dtype,
            if_exists="append", pk=["category_id"]
        )

        return [download_step, transform_step, load_step]


if __name__ == "__main__":
    dim_categories_pipeline = DimCategories()
    dim_categories_pipeline.run({})
