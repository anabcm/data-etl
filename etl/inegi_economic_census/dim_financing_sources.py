import pandas as pd

from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import Parameter, EasyPipeline, PipelineStep
from bamboo_lib.steps import DownloadStep, LoadStep


class TransformStep(PipelineStep):
    def run_step(self, prev, params):

        df = pd.read_csv(prev, sep=";", encoding="utf-8")
     
        return df


class DimFinancingSources(EasyPipeline):
    @staticmethod
    def steps(params):
        db_connector = Connector.fetch("clickhouse-database", open("../conns.yaml"))

        dtype = {
            "funding_source_id":   "UInt8",
            "funding_source_en":   "String",
            "funding_source_es":   "String",
        }

        download_step = DownloadStep(
            connector="dim-financing-source",
            connector_path="conns.yaml"
        )

        transform_step = TransformStep()

        load_step = LoadStep(
            "dim_financing_uses", db_connector, dtype=dtype,
            if_exists="append", pk=["funding_source_id"]
        )

        return [download_step, transform_step, load_step]


if __name__ == "__main__":
    dim_financing_sources_pipeline = DimFinancingSources()
    dim_financing_sources_pipeline.run({})
