
import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import Parameter, EasyPipeline, PipelineStep
from bamboo_lib.steps import DownloadStep, LoadStep


class ExtractStep(PipelineStep):
    def run_step(self, prev, params):
        df = pd.read_csv(prev)
        return df

class DimAgeRangePipeline(EasyPipeline):
    @staticmethod
    def steps(params):
        db_connector = Connector.fetch("clickhouse-database", open("../conns.yaml"))
        dtype = {
            "age":           "UInt8",
            "name_es":       "String",
            "name_en":       "String",
            "age_range_id":  "UInt8"
        }

        download_step = DownloadStep(
            connector='age-range-data',
            connector_path='conns.yaml'
        )

        extract_step = ExtractStep()
        load_step = LoadStep(
            "dim_age_range_enoe", db_connector, if_exists="drop", dtype=dtype,
            pk=["age"]
        )

        return [download_step, extract_step, load_step]

if __name__ == "__main__":
    pp = DimAgeRangePipeline()
    pp.run({})