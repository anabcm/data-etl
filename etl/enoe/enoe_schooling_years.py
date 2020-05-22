import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import DownloadStep
from bamboo_lib.steps import LoadStep


class ExtractStep(PipelineStep):
    def run_step(self, prev, params):
        df = pd.read_csv("https://docs.google.com/spreadsheets/d/e/2PACX-1vSKwGL6bkv7hJGc5szpkXmAV1rhRmkpN6i5W7-LdyUtpNntUtHJDzFDkiqX-AfhzBFaF2t5jB5VdsBo/pub?output=csv")
        return df

class DimSchoolingYearsPipeline(EasyPipeline):
    @staticmethod
    def steps(params):
        db_connector = Connector.fetch("clickhouse-database", open("../conns.yaml"))
        dtype = {
            "schooling_years":           "UInt8",
            "name_es":                   "String",
            "name_en":                   "String",
            "schooling_years_range_id":  "UInt8"
        }

        extract_step = ExtractStep()
        load_step = LoadStep(
            "dim_shared_enoe_schooling_years", db_connector, if_exists="drop", dtype=dtype,
            pk=["schooling_years"]
        )

        return [extract_step, load_step]

if __name__ == '__main__':
    pp = DimSchoolingYearsPipeline()
    pp.run({})
