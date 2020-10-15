import glob
import numpy as np
import os
import pandas as pd
import requests

from bamboo_lib.helpers import grab_parent_dir, query_to_df
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, PipelineStep, Parameter
from bamboo_lib.steps import DownloadStep, LoadStep, UnzipToFolderStep
from datetime import datetime

class TransformStep(PipelineStep):
    def run_step(self, prev, params):

        #Data from each report
        def _report(url):
            headers = {
                "Cache-Control": "no-cache",
                "Pragma": "no-cache"
            }
            r = requests.get(url, headers=headers)
            data_json = r.json()
            report = pd.DataFrame(data_json["data"])
            report = report.drop(columns=["Updated Date", "Covid Result", "Covid Result ID"])
            report = report.rename(columns={"Updated Date ID": "time_id"})
            report["reported_cases"] = report["Cases"].diff().fillna(0).astype(int)
            
            return report

        report_cases = _report("https://api.datamexico.org/tesseract/data.jsonrecords?Covid+Result=1&cube=gobmx_covid&drilldowns=Covid+Result%2CUpdated+Date&measures=Cases&parents=false&sparse=false")
        report_cases = report_cases.rename(columns={"reported_cases":"new_cases_report", "Cases":"accum_cases_report"})
        
        report_deaths = _report("https://api.datamexico.org/tesseract/data.jsonrecords?Covid+Result=1&Is+Dead=1&cube=gobmx_covid&drilldowns=Covid+Result%2CUpdated+Date&measures=Cases&parents=false&sparse=false")
        report_deaths = report_deaths.rename(columns={"reported_cases":"new_deaths_report", "Cases":"accum_deaths_report"})
        
        report_hospitalized = _report("https://api.datamexico.org/tesseract/data.jsonrecords?Covid+Result=1&Patient+Type=2&cube=gobmx_covid&drilldowns=Covid+Result%2CUpdated+Date&measures=Cases&parents=false&sparse=false")
        report_hospitalized = report_hospitalized.rename(columns={"reported_cases":"new_hospitalized_report", "Cases":"accum_hospitalized_report"})

        report_suspect = _report("https://api.datamexico.org/tesseract/data.jsonrecords?Covid+Result=3&cube=gobmx_covid&drilldowns=Covid+Result%2CUpdated+Date&measures=Cases&parents=false&sparse=false")
        report_suspect = report_suspect.rename(columns={"reported_cases":"new_suspect_report", "Cases":"accum_suspect_report"})

        report = pd.merge(report_cases, report_deaths, how="outer", on="time_id")
        report = pd.merge(report, report_hospitalized, how="outer", on="time_id")
        report = pd.merge(report, report_suspect, how="outer", on="time_id")

        #Data from latest report
        data = sorted(glob.glob("*.csv"))
        df = pd.read_csv(data[-1], encoding="latin-1")
        df.columns = [x.strip().lower().replace(" ", "_") for x in df.columns]

        #Hospitalized
        df_hosp = df[["fecha_ingreso", "resultado_lab", "tipo_paciente"]]
        df_hosp = df_hosp[(df_hosp["resultado_lab"] == 1) & (df_hosp["tipo_paciente"] == 2)]
        df_hosp = df_hosp.drop(columns="tipo_paciente")
        df_hosp = df_hosp.rename(columns={"fecha_ingreso":"time_id", "resultado_lab":"daily_hospitalized"})
        df_hosp = df_hosp.groupby(["time_id"]).sum().reset_index()

        #Suspect cases
        df_suspect = df[["fecha_ingreso", "resultado_lab"]]
        df_suspect = df_suspect[df_suspect["resultado_lab"] == 3]
        df_suspect["resultado_lab"] = df_suspect["resultado_lab"].replace(3,1)
        df_suspect = df_suspect.rename(columns={"fecha_ingreso":"time_id", "resultado_lab":"daily_suspect"})
        df_suspect = df_suspect.groupby(["time_id"]).sum().reset_index()

        #Cases
        df1 = df[["fecha_ingreso", "resultado_lab"]]
        df1 = df1[df1["resultado_lab"] == 1]
        df1 = df1.rename(columns={"fecha_ingreso":"time_id", "resultado_lab":"daily_cases"})
        df1 = df1.groupby(["time_id"]).sum().reset_index()

        df1 = pd.merge(df1, df_hosp, how="outer", on="time_id")
        df1 = pd.merge(df1, df_suspect, how="outer", on="time_id")
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
        df2 = df[["fecha_ingreso", "fecha_def", "resultado_lab"]]

        df2 = df2.rename(columns={"fecha_ingreso":"ingress_date", "fecha_def":"death_date", "resultado_lab":"daily_deaths"})

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

        #Merge daily cases and deaths
        data = pd.merge(df1_, df2_, how="outer", on="time_id")
        data = data.sort_values(by="time_id")

        data["time_id"] = data["time_id"].astype(str).str.replace("-", "").astype(int)

        #Merge data latest report with data from each report
        data = pd.merge(data, report, how="outer", on="time_id")

        for col in ["daily_cases", "daily_hospitalized", "daily_suspect", "daily_deaths", "accum_cases_report", "new_cases_report", "accum_deaths_report", "new_deaths_report", "accum_hospitalized_report", "new_hospitalized_report", "accum_suspect_report", "new_suspect_report"]:
            data[col] = data[col].fillna(0).astype(int)

        # Add column of accumulated and average cases
        data["accum_cases"] = data.daily_cases.cumsum()
        data["accum_deaths"] = data.daily_deaths.cumsum()
        data["accum_hospitalized"] = data.daily_hospitalized.cumsum()
        data["accum_suspect"] = data.daily_suspect.cumsum()

        for i in ["daily_cases", "accum_cases", "daily_deaths", "accum_deaths", "accum_cases_report", "new_cases_report", "accum_deaths_report", "new_deaths_report"]:
            measure = "avg7_{}".format(i)
            data[measure] = data[i].rolling(7).mean() 

        for j in ["daily_cases", "accum_cases", "daily_deaths", "accum_deaths", "accum_cases_report", "new_cases_report", "accum_deaths_report", "new_deaths_report"]:
            measure = "sum_last7_{}".format(j)
            data[measure] = data[j].rolling(7).sum()
        
        for k in ["sum_last7_daily_cases", "sum_last7_accum_cases", "sum_last7_daily_deaths", "sum_last7_accum_deaths", "sum_last7_accum_cases_report", "sum_last7_new_cases_report", "sum_last7_accum_deaths_report", "sum_last7_new_deaths_report"]:
            data[k] = data[k].fillna(0).astype(int)

        # Rate per 100.000 inhabitans
        data["population"] = 127792286

        data["rate_daily_cases"] = (data["daily_cases"] / data["population"]) * 100000
        data["rate_accum_cases"] = (data["accum_cases"] / data["population"]) * 100000
        data["rate_new_cases_report"] = (data["new_cases_report"] / data["population"]) * 100000
        data["rate_accum_cases_report"] = (data["accum_cases_report"] / data["population"]) * 100000

        data["rate_daily_deaths"] = (data["daily_deaths"] / data["population"]) * 100000
        data["rate_accum_deaths"] = (data["accum_deaths"] / data["population"]) * 100000
        data["rate_new_deaths_report"] = (data["new_cases_report"] / data["population"]) * 100000
        data["rate_accum_deaths_report"] = (data["accum_deaths_report"] / data["population"]) * 100000

        data = data.drop(columns="population")

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

        data["nation_id"] = "mex"

        print(datetime.now(), data["time_id"].max())

        return data


class CovidStatsNationPipeline(EasyPipeline):
    @staticmethod
    def steps(params):
        db_connector = Connector.fetch("clickhouse-database", open("../conns.yaml"))

        dtypes = {
            'time_id':                          "UInt32",
            'nation_id':                        "String",
            'daily_cases':                      "UInt32",
            'daily_deaths':                     "UInt32",
            'daily_hospitalized':               "UInt32",
            'daily_suspect':                    "UInt32",
            'accum_cases':                      "UInt32",
            'accum_deaths':                     "UInt32",
            'accum_hospitalized':               "UInt32",
            'accum_suspect':                    "UInt32",
            'days_between_ingress_and_death':   "Float32",
            'new_cases_report':                 "Int32",
            'new_deaths_report':                "Int32",
            'new_hospitalized_report':          "Int32",
            'new_suspect_report':               "Int32",   	
            'accum_cases_report':               "UInt32",
            'accum_deaths_report':              "UInt32",
            'accum_hospitalized_report':        "UInt32",
            'accum_suspect_report':             "UInt32",  
            'avg7_daily_cases':                 "Float32",
            'avg7_accum_cases':                 "Float32",
            'avg7_daily_deaths':                "Float32",
            'avg7_accum_deaths':                "Float32",
            'avg7_new_cases_report':            "Float32",
            'avg7_accum_cases_report':          "Float32",
            'avg7_new_deaths_report':           "Float32",
            'avg7_accum_deaths_report':         "Float32",
            'sum_last7_daily_cases':            "UInt16",
            'sum_last7_daily_deaths':           "UInt16",
            'sum_last7_accum_cases':            "UInt16",
            'sum_last7_accum_deaths':           "UInt16",
            'sum_last7_new_cases_report':       "UInt16",
            'sum_last7_accum_cases_report':     "UInt16",
            'sum_last7_new_deaths_report':      "UInt16",
            'sum_last7_accum_deaths_report':    "UInt16",
            'rate_daily_cases':                 "Float32",
            'rate_accum_cases':                 "Float32",
            'rate_daily_deaths':                "Float32",
            'rate_accum_deaths':                "Float32",
            'rate_new_cases_report':            "Float32",
            'rate_accum_cases_report':          "Float32",
            'rate_new_deaths_report':           "Float32",
            'rate_accum_deaths_report':         "Float32",
            'day_from_50_cases':                "UInt16",
            'day_from_10_deaths':               "UInt16"
        }

        xform_step = TransformStep(connector=db_connector)
        load_step = LoadStep(
            "gobmx_covid_stats_nation", db_connector, if_exists="drop", 
            pk=["time_id", "nation_id"], 
            nullable_list=['days_between_ingress_and_death', 
                            'avg7_daily_cases',
                            'avg7_accum_cases',
                            'avg7_daily_deaths',
                            'avg7_accum_deaths',
                            'avg7_new_cases_report',
                            'avg7_accum_cases_report',
                            'avg7_new_deaths_report',
                            'avg7_accum_deaths_report',
                            'sum_last7_daily_cases',
                            'sum_last7_daily_deaths',
                            'sum_last7_accum_cases',
                            'sum_last7_accum_deaths',
                            'sum_last7_new_cases_report',
                            'sum_last7_accum_cases_report',
                            'sum_last7_new_deaths_report',
                            'sum_last7_accum_deaths_report',
                            'rate_daily_cases',
                            'rate_accum_cases',
                            'rate_daily_deaths',
                            'rate_accum_deaths',
                            'rate_new_cases_report',
                            'rate_accum_cases_report',
                            'rate_new_deaths_report',
                            'rate_accum_deaths_report'], 
            dtype=dtypes
        )

        return [xform_step, load_step]

if __name__ == "__main__":
    pp = CovidStatsNationPipeline()
    pp.run({})