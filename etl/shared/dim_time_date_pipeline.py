import pandas as pd
import math
from bamboo_lib.models import PipelineStep, AdvancedPipelineExecutor
from bamboo_lib.models import Parameter, EasyPipeline
from bamboo_lib.connectors.models import Connector
from bamboo_lib.steps import LoadStep

class CreateStep(PipelineStep):
    def run_step(self, prev, params):
        start = "1990-01-01"
        end = "2030-12-31"
        df = pd.DataFrame({"date": pd.date_range(start, end)})

        df["month"] = df.date.dt.year.astype(str) + "-" + df.date.dt.month.astype(str).str.zfill(2)
        df["month_id"] = (df.date.dt.year.astype(str) + df.date.dt.month.astype(str).str.zfill(2)).astype(int)

        df["quarter"] = df.date.dt.year.astype(str) + "-Q" + df.date.dt.quarter.astype(str)
        df["quarter_id"] = (df.date.dt.year.astype(str) + df.date.dt.quarter.astype(str)).astype(int)

        df["year"] = df.date.dt.year

        df.insert(0, "date_id", (df.date.dt.year.astype(str) + df.date.dt.month.astype(str).str.zfill(2) + df.date.dt.day.astype(str).str.zfill(2)).astype(int))
        df["date"] = pd.to_datetime(df["date"]).dt.date

        return df

class DimTimeDatePipeline(EasyPipeline):
    @staticmethod
    def pipeline_id():
        return "date-month-pipeline"

    @staticmethod
    def name():
        return "Shared dimension"

    @staticmethod
    def description():
        return "Creates date month dimension table"

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
            "quarter_id":       "UInt16",
            "quarter":          "String",
            "month_id":         "UInt32",
            "month":            "String",
            "date_id":          "UInt32",
            "date":             "String",
            "year":             "UInt16"
        }

        # Definition of each step
        create_step = CreateStep()
        load_step = LoadStep("dim_shared_date_month_day", db_connector, if_exists="drop", pk=["month_id"], dtype=dtype)
        
        return [create_step, load_step]