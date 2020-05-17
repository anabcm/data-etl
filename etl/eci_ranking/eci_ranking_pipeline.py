import json

import pandas as pd
import requests
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import Parameter, EasyPipeline, PipelineStep
from bamboo_lib.steps import DownloadStep, LoadStep


class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        with open(prev, "r") as file:
            data = json.loads(file.read())

        df = pd.DataFrame(data["data"])

        df = df.drop(columns="Country")
        df = df.rename(columns={
            "Country ID": "country_id",
            "Trade Value ECI": "eci",
            "Trade Value ECI Ranking": "eci_rank"
        })

        df["year"] = params.get("year")

        df = df[["year", "country_id", "eci_rank", "eci"]]

        return df


class ECIPipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return [
            Parameter(label="Version", name="version", dtype=str),
            Parameter(label="Years", name="years", dtype=str),
            Parameter(label="Threshold_country", name="threshold_country", dtype=str),
            Parameter(label="Threshold_HS", name="threshold_HS", dtype=str),
            Parameter(label="Year", name="year", dtype=str),
            Parameter(label="Level", name="level", dtype=str),
        ]

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch("clickhouse-database", open("../conns.yaml"))

        dtype = {
            "year":             "UInt16",
            "country_id":       "String",
            "eci_rank":         "UInt16",
            "eci":              "Float64",
        }

        download_step = DownloadStep(
            connector="eci_ranking",
            connector_path="conns.yaml"
        )

        transform_step = TransformStep()

        load_step = LoadStep(
            "complexity_eci_a_hs{}_hs{}".format(params['version'], params['level']), db_connector, dtype=dtype, if_exists="append",
            pk=["year", "country_id", "eci_rank"]
        )

        return [download_step, transform_step, load_step]


if __name__ == "__main__":
    eci_pipeline = ECIPipeline()

    dict_year_start = {"12": 2012}
    hs_version = ["12"]

    for level in ["4", "6"]:
        for version in hs_version:
            start_year = dict_year_start[version]

            for year in range(start_year, 2018+1):
                one_year=year-1
                two_year=year-2

                if year == start_year:
                    threshold_country=1000000000
                    threshold_HS=500000000
                    years=year

                elif year == start_year+1:
                    threshold_country=2000000000
                    threshold_HS=1000000000
                    years=str(one_year)+","+str(year)

                else:
                    threshold_country=3000000000
                    threshold_HS=1500000000
                    years=str(two_year)+","+str(one_year)+","+str(year)

                eci_pipeline.run({
                    "version": version,
                    "years": years,
                    "threshold_country":threshold_country,
                    "threshold_HS":threshold_HS,
                    "year":year,
                    "level":level
                })