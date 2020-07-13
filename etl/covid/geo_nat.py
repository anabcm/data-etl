import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import DownloadStep
from bamboo_lib.steps import LoadStep


class ExtractStep(PipelineStep):
    def run_step(self, prev, params):
        data = {
            "nation_id": ["mex"],
            "name_es": ["México"]
        }
        df = pd.DataFrame.from_dict(data)

        return df

class NatGeoCovidPipeline(EasyPipeline):
    @staticmethod
    def steps(params):
        db_connector = Connector.fetch("clickhouse-database", open("../conns.yaml"))
        dtype = {
            "nation_id":     "String",
            "name_es":       "String"
        }

        extract_step = ExtractStep()
        load_step = LoadStep(
            "dim_geo_covid_nat", db_connector, if_exists="drop", dtype=dtype,
            pk=["nation_id"]
        )

        return [extract_step, load_step]

if __name__ == "__main__":
    pp = NatGeoCovidPipeline()
    pp.run({})