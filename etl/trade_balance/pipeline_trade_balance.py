import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import DownloadStep
from bamboo_lib.steps import LoadStep

class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        df = pd.read_csv(prev)
        df = df[~df["location_code"].isnull()]

        for col_name in ["location_code", "product_code", "year", "export_num_plants", "import_num_plants"]:
            df[col_name] = df[col_name].astype(int)
        df["country_code"] = df["country_code"].str.lower()

        df = df[["year", "export_value", "import_num_plants", "export_num_plants", "import_value", "location_code", "country_code", "product_code", "pci"]]
        df = df.rename(columns={"location_code": "mun_id", "product_code": "product_id", "country_code": "country_id"})

        return df

class TradeBalanceComplexityPipeline(EasyPipeline):
    @staticmethod
    def steps(params):
        db_connector = Connector.fetch("clickhouse-database", open("../conns.yaml"))

        dl_step = DownloadStep(connector="dataset", connector_path=__file__)
        transform_step = TransformStep()
        load_step = LoadStep(
            "trade_balance_atlas", db_connector, if_exists="drop", dtype={"product_id": "UInt16", "mun_id": "UInt16"},
            pk=["mun_id", "product_id"]
        )

        return [transform_step, load_step]