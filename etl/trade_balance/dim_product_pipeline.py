import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import DownloadStep
from bamboo_lib.steps import LoadStep

class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        df = pd.read_csv("https://docs.google.com/spreadsheets/d/e/2PACX-1vT0AjDY30SASFyGAEL9yYmuIB_vG8YehV_QUrjCvnssr-U-TLr1lDXxSF-3lAsk9qDnz8VOBeA9YkFC/pub?output=csv")
        return df

class DimProductPipeline(EasyPipeline):
    @staticmethod
    def steps(params):
        db_connector = Connector.fetch("clickhouse-database", open("../conns.yaml"))

        transform_step = TransformStep()
        load_step = LoadStep(
            "dim_shared_product", db_connector, if_exists="drop", dtype={"product_id": "UInt16"},
            pk=["product_id"]
        )

        return [transform_step, load_step]