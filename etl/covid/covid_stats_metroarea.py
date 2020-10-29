
import glob
import numpy as np
import os
import pandas as pd
import requests

from bamboo_lib.helpers import grab_parent_dir
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, PipelineStep, Parameter
from bamboo_lib.steps import DownloadStep, LoadStep, UnzipToFolderStep
from datetime import datetime


class TransformStep(PipelineStep):
    def run_step(self, prev, params):

        #Create dictionary for population metroarea
        r = requests.get("https://api.datamexico.org/tesseract/data?Year=2020&cube=population_projection&drilldowns=Metro+Area&measures=Projected+Population")
        data_json = r.json()
        mz_population = pd.DataFrame(data_json["data"])
        dicto_mz_population = dict(zip(mz_population["Metro Area ID"], mz_population["Projected Population"]))

        #Metroareas ids
        zm_names = pd.read_csv(prev, encoding="latin-1")
        zm_names = zm_names[["CVE_ZM", "NOM_ZM", "CVE_MUN"]]
        zm_names = zm_names.rename(columns={"CVE_ZM": "zm_id", "CVE_MUN": "mun_id"})
        zm_names["zm_id"] = ("99" + zm_names["zm_id"].astype(str).str.replace(".", "")).astype(int)

        #Data from latest report
        data = sorted(glob.glob("*.csv"))
        df = pd.read_csv(data[-1], encoding="latin-1")
        df.columns = [x.strip().lower().replace(" ", "_") for x in df.columns]

        df["entidad_res"] = df["entidad_res"].astype(str).str.zfill(2)
        df["municipio_res"] = df["municipio_res"].astype(str).str.zfill(3)
        df["mun_id"] = (df["entidad_res"] + df["municipio_res"]).astype(int)

        # ids refactor
        df.loc[df['clasificacion_final'].isin([1,2,3]), 'resultado_lab'] = 1
        df.loc[df['clasificacion_final'].isin([4,5,6]), 'resultado_lab'] = 3
        df.loc[df['clasificacion_final'] == 7, 'resultado_lab'] = 2

        #Cases
        df1 = df[["mun_id", "fecha_ingreso", "resultado_lab"]]
        df1 = df1[df1["resultado_lab"] == 1]
        df1 = df1.rename(columns={"fecha_ingreso":"time_id", "resultado_lab":"daily_cases"})
        df1 = df1.groupby(["mun_id","time_id"]).sum().reset_index()

        df1["time_id"] = pd.to_datetime(df1["time_id"])

        df1 = pd.merge(df1, zm_names, how="right", on=["mun_id"])
        df1 = df1.drop(columns = "mun_id")
        df1 = df1.groupby(["zm_id","time_id"]).sum().reset_index()

        #Add missing dates daily cases
        df1_ = []
        last_day = df1["time_id"].max()
        for a, df_a in df1.groupby("zm_id"):
            
            first_day = df_a["time_id"].min()
            idx = pd.date_range(first_day, last_day)
            df_b = df_a.reindex(idx)
            df_b = pd.DataFrame(df_b.index)
            df_b = df_b.rename(columns={0:"time_id"})
            
            result = pd.merge(df_b, df_a, how="outer", on="time_id")
            
            df1_.append(result)
        df1_ = pd.concat(df1_, sort=False)

        df1_["zm_id"] = df1_["zm_id"].fillna(method="ffill")

        #Deaths
        df2 = df[["mun_id", "fecha_def", "resultado_lab"]]
        df2 = df2.rename(columns={"fecha_def":"time_id", "resultado_lab":"daily_deaths"})
        df2 = df2[df2["daily_deaths"] == 1]
        df2 = df2[df2["time_id"] != "9999-99-99"]
        df2 = df2.groupby(["mun_id","time_id"]).sum().reset_index()

        df2["time_id"] = pd.to_datetime(df2["time_id"])

        df2 = pd.merge(df2, zm_names, how="right", on=["mun_id"])
        df2 = df2.drop(columns = "mun_id")

        df2 = df2.dropna()
        df2 = df2.groupby(["zm_id","time_id"]).sum().reset_index()

        #Add missing dates deaths 
        df2_ = []
        last_day = df2["time_id"].max()
        for a, df_a in df2.groupby("zm_id"):
            
            first_day = df_a["time_id"].min()
            idx = pd.date_range(first_day, last_day)
            df_b = df_a.reindex(idx)
            df_b = pd.DataFrame(df_b.index)
            df_b = df_b.rename(columns={0:"time_id"})
            
            result = pd.merge(df_b, df_a, how="outer", on="time_id")
            
            df2_.append(result)
        df2_ = pd.concat(df2_, sort=False)

        df2_["zm_id"] = df2_["zm_id"].fillna(method="ffill")

        #Merge daily cases and deaths
        data = pd.merge(df1_, df2_, how="outer", on=["time_id", "zm_id"])
        data = data.sort_values(by=["time_id", "zm_id"])

        data["time_id"] = data["time_id"].astype(str).str.replace("-", "").astype(int)
        data["zm_id"] = data["zm_id"].astype(int)

        for col in ["daily_cases", "daily_deaths"]:
            data[col] = data[col].fillna(0).astype(int)

        #Add column of accumulated cases
        df_final = []
        for a, df_a in data.groupby("zm_id"):
            _df = df_a.copy()
            _df["accum_cases"] = _df.daily_cases.cumsum()
            _df["accum_deaths"] = _df.daily_deaths.cumsum()
            
            for i in ["daily_cases", "accum_cases", "daily_deaths", "accum_deaths"]:
                measure = "avg7_{}".format(i)
                _df[measure] = _df[i].rolling(7).mean()
                
            for j in ["daily_cases", "accum_cases", "daily_deaths", "accum_deaths"]:
                measure = "sum_last7_{}".format(j)
                _df[measure] = _df[j].rolling(7).sum()
            
            df_final.append(_df)
        df_final = pd.concat(df_final, sort=False)

        for k in ["sum_last7_daily_cases", "sum_last7_accum_cases", "sum_last7_daily_deaths", "sum_last7_accum_deaths"]:
            df_final[k] = df_final[k].fillna(0).astype(int)

        #Rates by population
        df_final["population"] = df_final["zm_id"].replace(dicto_mz_population)

        df_final["rate_daily_cases"] = (df_final["daily_cases"] / df_final["population"]) * 100000
        df_final["rate_accum_cases"] = (df_final["accum_cases"] / df_final["population"]) * 100000

        df_final["rate_daily_deaths"] = (df_final["daily_deaths"] / df_final["population"]) * 100000
        df_final["rate_accum_deaths"] = (df_final["accum_deaths"] / df_final["population"]) * 100000

        df_final = df_final.drop(columns={"population"})

        #Add a column with days, being day one when at least 50 cases accumulate
        day = []
        for a, df_a in df_final.groupby("zm_id"):
            default = 0
            for row in df_a.iterrows():
                if row[1]["accum_cases"] >= 50:
                    default = default + 1

                day.append(default)
        df_final["day_from_50_cases"] = day

        #Add a column with days, being day one when at least 10 deaths accumulate
        day = []
        for a, df_a in df_final.groupby("zm_id"):
            default = 0
            for row in df_a.iterrows():
                if row[1]["accum_deaths"] >= 10:
                    default = default + 1

                day.append(default)
        df_final["day_from_10_deaths"] = day

        print(datetime.now(), df_final["time_id"].max())

        return df_final


class CovidStatsMetroareaPipeline(EasyPipeline):
    @staticmethod
    def steps(params):
        db_connector = Connector.fetch("clickhouse-database", open("../conns.yaml"))

        dtypes = {
            'time_id':                          "UInt32",
            'zm_id':                            "UInt32",
            'daily_cases':                      "UInt32",
            'daily_deaths':                     "UInt32",
            'accum_cases':                      "UInt32",
            'accum_deaths':                     "UInt32",
            'avg7_daily_cases':                 "Float32",
            'avg7_accum_cases':                 "Float32",
            'avg7_daily_deaths':                "Float32",
            'avg7_accum_deaths':                "Float32",
            'sum_last7_daily_cases':            "Int32",
            'sum_last7_daily_deaths':           "Int32",
            'sum_last7_accum_cases':            "Int32",
            'sum_last7_accum_deaths':           "Int32",
            'rate_daily_cases':                 "Float32",
            'rate_accum_cases':                 "Float32",
            'rate_daily_deaths':                "Float32",
            'rate_accum_deaths':                "Float32",
            'day_from_50_cases':                "UInt16",
            'day_from_10_deaths':               "UInt16"
        }

        download_step = DownloadStep(
            connector='zm-data',
            connector_path="conns.yaml"
        )

        xform_step = TransformStep()
        load_step = LoadStep(
            "gobmx_covid_stats_metroarea", db_connector, if_exists="drop", 
            pk=["time_id", "zm_id"], 
            nullable_list=[
                            'avg7_daily_cases',
                            'avg7_accum_cases',
                            'avg7_daily_deaths',
                            'avg7_accum_deaths'], 
            dtype=dtypes
        )

        return [download_step, xform_step, load_step]

if __name__ == "__main__":
    pp = CovidStatsMetroareaPipeline()
    pp.run({})