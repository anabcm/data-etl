import pandas as pd
import math
from bamboo_lib.models import PipelineStep, AdvancedPipelineExecutor
from bamboo_lib.models import Parameter, EasyPipeline
from bamboo_lib.connectors.models import Connector
from bamboo_lib.steps import LoadStep

class CreateStep(PipelineStep):
    def run_step(self, prev, params):
        # "yyyy-mm-dd"
        start = params.get("init")
        end = params.get("end")
        df = pd.DataFrame({"date": pd.date_range(start, end)})

        df["month_name"] = df.date.dt.year.astype(str) + "-" + df.date.dt.month.astype(str).str.zfill(2)
        df["month"] = df.date.dt.month

        df["quarter_name"] = "Q" + df.date.dt.quarter.astype(str)
        df["quarter"] = df.date.dt.quarter

        df["year"] = df.date.dt.year

        df.insert(0, "date_id", (df.date.dt.year.astype(str) + df.date.dt.month.astype(str).str.zfill(2) + df.date.dt.day.astype(str).str.zfill(2)).astype(int))
        df["day"] = pd.to_datetime(df["date"]).dt.day
        df["time_id"] = df["date_id"]
        df["time_name"] = df["date"].dt.date.astype("str")
        df = df[["day", "month", "month_name", "quarter", "quarter_name", "year", "time_id", "time_name"]].copy()

        return df

class DimTimeDateCovidStatsPipeline(EasyPipeline):
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
            Parameter(label="Initial-date", name="init", dtype=str),
            Parameter(label="Final-date", name="end", dtype=str)
        ]

    @staticmethod
    def steps(params):
        # Use of connectors specified in the conns.yaml file
        db_connector = Connector.fetch("clickhouse-database", open("../conns.yaml"))

        dtype = {
            "day":              "UInt8",
            "day_name":         "String",
            "month":            "UInt8",
            "month_name":       "String",
            "quarter":          "UInt8",
            "quarter_name":     "String",
            "year":             "UInt16",
            "time_id":          "UInt32",
            "day_name":         "String",      
        }

        # Definition of each step
        create_step = CreateStep()
        load_step = LoadStep("dim_date_covid_stats", db_connector, if_exists="drop", 
          pk=["time_id"], dtype=dtype)
        
        return [create_step, load_step]

if __name__ == "__main__":
    pp = DimTimeDateCovidStatsPipeline()
    pp.run({})