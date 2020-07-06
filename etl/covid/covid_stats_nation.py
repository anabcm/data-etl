import glob
import numpy as np
import os
import pandas as pd
import requests

from bamboo_lib.helpers import grab_parent_dir, query_to_df
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, PipelineStep, Parameter
from bamboo_lib.steps import DownloadStep, LoadStep, UnzipToFolderStep


class TransformStep(PipelineStep):
    def run_step(self, prev, params):

        data = sorted(glob.glob("*.csv"))

        df = pd.read_csv(data[-1], encoding="latin-1")
        df.columns = [x.strip().lower().replace(" ", "_") for x in df.columns]

        df1 = df[["fecha_ingreso", "resultado"]]
        df1 = df1[df1["resultado"] == 1]
        df1 = df1.rename(columns={"fecha_ingreso":"time_id", "resultado":"daily_cases"})

        df1 = df1.groupby(["time_id"]).sum().reset_index()
        df1["time_id"] = pd.to_datetime(df1["time_id"])

        # Add missing dates daily cases
        last_day = df1["time_id"].max()
        first_day = df1["time_id"].min()

        idx = pd.date_range(first_day, last_day)
        df_temp = df1.reindex(idx)
        df_temp = pd.DataFrame(df_temp.index)
        df_temp = df_temp.rename(columns={0:"time_id"})

        df1_ = pd.merge(df_temp, df1, how="outer", on="time_id")

        # Deaths
        df2 = df[["fecha_ingreso", "fecha_def", "resultado"]]

        df2 = df2.rename(columns={"fecha_ingreso":"ingress_date", "fecha_def":"death_date", "resultado":"daily_deaths"})

        df2 = df2[df2["daily_deaths"] == 1]
        df2 = df2[df2["death_date"]!= "9999-99-99"]

        for i in ["ingress_date", "death_date"]:
            df2[i] = pd.to_datetime(df2[i])
            
        df2["days_between_ingress_and_death"] = df2["death_date"] - df2["ingress_date"]
        df2["days_between_ingress_and_death"] = df2["days_between_ingress_and_death"].dt.days

        df2 = df2.drop(columns="ingress_date")
        df2 = df2.rename(columns={"death_date":"time_id"})

        df2 = df2.groupby(["time_id"]).agg({"daily_deaths":"sum", "days_between_ingress_and_death": "mean"}).reset_index()

        # Add missing dates deaths 
        last_day = df2["time_id"].max()
        first_day = df2["time_id"].min()

        idx = pd.date_range(first_day, last_day)
        df_temp = df2.reindex(idx)
        df_temp = pd.DataFrame(df_temp.index)
        df_temp = df_temp.rename(columns={0:"time_id"})

        df2_ = pd.merge(df_temp, df2, how="outer", on="time_id")

        # Merge daily cases and deaths
        data = pd.merge(df1_, df2_, how="outer", on="time_id")
        data = data.sort_values(by="time_id")

        for col in ["daily_cases", "daily_deaths"]:
            data[col] = data[col].fillna(0).astype(int)

        # Add column of accumulated and average cases
        data["accum_cases"] = data.daily_cases.cumsum()
        data["accum_deaths"] = data.daily_deaths.cumsum()

        for i in ["daily_cases", "accum_cases", "daily_deaths", "accum_deaths"]:
            measure = "avg_7_days_{}".format(i)
            data[measure] = data[i].rolling(7).mean()

        for j in ["daily_cases", "daily_deaths"]:
            measure = "total_last_7_days_{}".format(j)
            data[measure] = data[j].rolling(7).sum()

        data = data.rename(columns={"total_last_7_days_daily_cases":"cases_last_7_days", "total_last_7_days_daily_deaths":"deaths_last_7_days"})

        for i in ["cases_last_7_days", "deaths_last_7_days"]:
            data[i] = data[i].fillna(0).astype(int)

        # Rate per 100.000 inhabitans
        data["population"] = 127792286

        data["rate_daily_cases"] = (data["daily_cases"] / data["population"]) * 100000
        data["rate_accum_cases"] = (data["accum_cases"] / data["population"]) * 100000

        data["rate_daily_deaths"] = (data["daily_deaths"] / data["population"]) * 100000
        data["rate_accum_deaths"] = (data["accum_deaths"] / data["population"]) * 100000

        data = data.drop(columns="population")

        # Add column with day from first case and death
        for col in ["accum_cases", "accum_deaths"]:
            measure = "{}_day".format(col)
            day = []
            default = 0
            
            for row in data.iterrows():
                if row[1][col] != 0:
                    default = default + 1

                day.append(default)
            data[measure] = day 
            
        data = data.rename(columns={"accum_cases_day": "cases_day", "accum_deaths_day": "deaths_day"})

        # Add a column with days, being day one when at least 50 cases accumulate
        day = []
        default = 0

        for row in data.iterrows():
            if row[1]["accum_cases"] >= 50:
                default = default + 1

            day.append(default)
        data["day_from_50_cases"] = day

        # Add a column with days, being day one when at least 10 deaths accumulate
        day_ = []
        default_ = 0
        for row in data.iterrows():
            if row[1]["accum_deaths"] >= 10:
                default_ = default_ + 1

            day_.append(default_)
        data["day_from_10_deaths"] = day_

        data["time_id"] = data["time_id"].astype(str).str.replace("-", "").astype(int)
        data["nation_id"] = "mex"

        return data


class CovidStatsNationPipeline(EasyPipeline):
    @staticmethod
    def steps(params):
        db_connector = Connector.fetch("clickhouse-database", open("../conns.yaml"))

        dtypes = {
            "time_id":                          "UInt32",
            "nation_id":                        "String",
            "daily_cases":                      "UInt32",
            "daily_deaths":                     "UInt32",
            "days_between_ingress_and_death":   "Float32",
            "accum_cases":                      "UInt32",
            "accum_deaths":                     "UInt32",
            "rate_daily_cases":                 "Float32",
            "rate_accum_cases":                 "Float32",
            "rate_daily_deaths":                "Float32",
            "rate_accum_deaths":                "Float32",
            "avg_7_days_daily_cases":           "Float32",
            "avg_7_days_accum_cases":           "Float32",
            "avg_7_days_daily_deaths":          "Float32",
            "avg_7_days_accum_deaths":          "Float32",
            "cases_day":                        "UInt16",
            "deaths_day":                       "UInt16",
            "day_from_50_cases":                "UInt16",
            "day_from_10_deaths":               "UInt16",
            "cases_last_7_days":                "UInt16",
            "deaths_last_7_days":               "UInt16",
        }

        download_step = DownloadStep(
            connector="covid-data-mx",
            connector_path="conns.yaml",
            force=True
        )

        path = grab_parent_dir(".") + "/covid/"
        unzip_step = UnzipToFolderStep(compression="zip", target_folder_path=path)
        xform_step = TransformStep(connector=db_connector)
        load_step = LoadStep(
            "gobmx_covid_stats_nation", db_connector, if_exists="drop", 
            pk=["time_id"], 
            nullable_list=["days_between_ingress_and_death", "avg_7_days_daily_cases", "avg_7_days_accum_cases", 
                           "avg_7_days_daily_deaths", "avg_7_days_accum_deaths", "rate_daily_cases", "rate_accum_cases", 
                           "rate_daily_deaths", "rate_accum_deaths"], 
            dtype=dtypes
        )

        return [download_step, unzip_step, xform_step, load_step]

if __name__ == "__main__":
    pp = CovidStatsNationPipeline()
    pp.run({})