import pandas as pd
from bamboo_lib.models import PipelineStep, AdvancedPipelineExecutor
from bamboo_lib.models import Parameter, EasyPipeline
from bamboo_lib.connectors.models import Connector
from bamboo_lib.steps import LoadStep

class CreateStep(PipelineStep):
    def run_step(self, prev, params):
        data = []

        for year in range(2000, 2030 + 1):
            for quarter in range(1, 4 + 1):
                data.append({
                    "year": year,
                    "quarter_name": "{}-Q{}".format(year, quarter),
                    "quarter_id": int("{}{}".format(year, quarter))
                })

        return pd.DataFrame(data)

class DimTimeQuarterPipeline(EasyPipeline):
    @staticmethod
    def pipeline_id():
        return "datetime-pipeline"

    @staticmethod
    def name():
        return "Shared dimension"

    @staticmethod
    def description():
        return "Creates date quarter dimension table"

    @staticmethod
    def website():
        return "http://datawheel.us"

    @staticmethod
    def parameter_list():
        return [
            Parameter(label="Source connector", name="source-connector", dtype=str, source=Connector)
        ]

    @staticmethod
    def steps(params):
        # Use of connectors specified in the conns.yaml file
        db_connector = Connector.fetch("clickhouse-database", open("../conns.yaml"))

        dtype = {
            "quarter_id":         "UInt16",
            "quarter_name":       "String",
            "year":               "UInt16"
        }

        # Definition of each step
        create_step = CreateStep()
        load_step = LoadStep("dim_shared_date_quarter", db_connector, if_exists="drop", pk=["date_id"], dtype=dtype)
        
        return [create_step, load_step]