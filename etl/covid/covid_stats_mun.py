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

        #Data from each report
        def _report(url):
            r = requests.get(url)
            data_json = r.json()

            report = pd.DataFrame(data_json["data"])
            report = report[report["Municipality"] != "No Informado"]
            
            report = report.drop(columns={"Updated Date", "Municipality"})
            report = report.rename(columns={"Updated Date ID": "time_id", "Municipality ID":"mun_id"})
            
            df_temp = []
            for a, df_a in report.groupby("mun_id"):
                _df = df_a.copy()
                _df = _df.sort_values("time_id")
                _df["reported_cases"] = _df["Cases"].diff().fillna(0).astype(int)

                df_temp.append(_df)
            df_temp = pd.concat(df_temp, sort=False)
            
            return df_temp

        report_cases = _report("https://api.datamexico.org/tesseract/data?Covid+Result=1&cube=gobmx_covid&drilldowns=Updated+Date%2CMunicipality&measures=Cases")
        report_cases = report_cases.rename(columns={"reported_cases":"new_cases_report", "Cases":"accum_cases_report"})

        report_deaths = _report("https://api.datamexico.org/tesseract/data?Covid+Result=1&Is+Dead=1&cube=gobmx_covid&drilldowns=Updated+Date%2CMunicipality&measures=Cases")
        report_deaths = report_deaths.rename(columns={"reported_cases":"new_deaths_report", "Cases":"accum_deaths_report"})

        report_hospitalized = _report("https://api.datamexico.org/tesseract/data?Covid+Result=1&Patient+Type=2&cube=gobmx_covid&drilldowns=Updated+Date%2CMunicipality&measures=Cases")
        report_hospitalized = report_hospitalized.rename(columns={"reported_cases":"new_hospitalized_report", "Cases":"accum_hospitalized_report"})

        report_suspect = _report("https://api.datamexico.org/tesseract/data?Covid+Result=3&cube=gobmx_covid&drilldowns=Updated+Date%2CMunicipality&measures=Cases")
        report_suspect = report_suspect.rename(columns={"reported_cases":"new_suspect_report", "Cases":"accum_suspect_report"})

        report = pd.merge(report_cases, report_deaths, how="outer", on=["time_id", "mun_id"])
        report = pd.merge(report, report_hospitalized, how="outer", on=["time_id", "mun_id"])
        report = pd.merge(report, report_suspect, how="outer", on=["time_id", "mun_id"])

        #Create dictionary for state and population
        r = requests.get("https://api.datamexico.org/tesseract/data?Year=2020&cube=population_projection&drilldowns=Municipality&measures=Projected+Population&parents=false&sparse=false")
        data_json = r.json()
        states_data = pd.DataFrame(data_json["data"])
        dicto_mun_population = dict(zip(states_data["Municipality ID"], states_data["Projected Population"]))

        #Data from latest report
        data = sorted(glob.glob("*.csv"))
        df = pd.read_csv(data[-1], encoding="latin-1")
        df.columns = [x.strip().lower().replace(" ", "_") for x in df.columns]

        df["entidad_res"] = df["entidad_res"].astype(str).str.zfill(2)
        df["municipio_res"] = df["municipio_res"].astype(str).str.zfill(3)
        df["mun_id"] = (df["entidad_res"] + df["municipio_res"]).astype(int)

        #Suspect cases
        df_suspect = df[["mun_id", "fecha_ingreso", "resultado"]]
        df_suspect = df_suspect[df_suspect["resultado"] == 3]
        df_suspect["resultado"] = df_suspect["resultado"].replace(3,1)
        df_suspect = df_suspect.rename(columns={"fecha_ingreso":"time_id", "resultado":"daily_suspect"})
        df_suspect = df_suspect.groupby(["mun_id","time_id"]).sum().reset_index()

        #Hospitalized
        df_hosp = df[["mun_id", "fecha_ingreso", "resultado", "tipo_paciente"]]
        df_hosp = df_hosp[(df_hosp["resultado"] == 1) & (df_hosp["tipo_paciente"] == 2)]
        df_hosp = df_hosp.drop(columns="tipo_paciente")
        df_hosp = df_hosp.rename(columns={"fecha_ingreso":"time_id", "resultado":"daily_hospitalized"})
        df_hosp = df_hosp.groupby(["mun_id","time_id"]).sum().reset_index()

        #Cases
        df1 = df[["mun_id", "fecha_ingreso", "resultado"]]
        df1 = df1[df1["resultado"] == 1]
        df1 = df1.rename(columns={"fecha_ingreso":"time_id", "resultado":"daily_cases"})
        df1 = df1.groupby(["mun_id","time_id"]).sum().reset_index()

        df1 = pd.merge(df1, df_hosp, how="outer", on=["mun_id","time_id"])
        df1 = pd.merge(df1, df_suspect, how="outer", on=["mun_id","time_id"])
        df1["time_id"] = pd.to_datetime(df1["time_id"])

        # Replace unknown municipalities
        mun = query_to_df(self.connector, "select mun_id from dim_shared_geography_mun")
        df1.loc[~df1["mun_id"].isin(mun['mun_id']), "mun_id"] = 33000

        df1 = df1.groupby(["mun_id", "time_id"]).sum().reset_index()
        df1["time_id"] = pd.to_datetime(df1["time_id"])

        # Add missing dates daily cases
        df1_ = []
        last_day = df1["time_id"].max()
        for a, df_a in df1.groupby("mun_id"):
            
            first_day = df_a["time_id"].min()
            idx = pd.date_range(first_day, last_day)
            df_b = df_a.reindex(idx)
            df_b = pd.DataFrame(df_b.index)
            df_b = df_b.rename(columns={0: "time_id"})
            
            result = pd.merge(df_b, df_a, how="outer", on="time_id")
            
            df1_.append(result)
        df1_ = pd.concat(df1_, sort=False)

        df1_["mun_id"] = df1_["mun_id"].fillna(method="ffill")

        #Deaths
        df2 = df[["mun_id", "fecha_ingreso", "fecha_def", "resultado"]]

        df2 = df2.rename(columns={"fecha_ingreso": "ingress_date", 
                                  "fecha_def": "death_date", 
                                  "resultado": "daily_deaths"})

        df2 = df2[df2["daily_deaths"] == 1]
        df2 = df2[df2["death_date"]!= "9999-99-99"]

        for i in ["ingress_date", "death_date"]:
            df2[i] = pd.to_datetime(df2[i])
            
        df2["days_between_ingress_and_death"] = df2["death_date"] - df2["ingress_date"]
        df2["days_between_ingress_and_death"] = df2["days_between_ingress_and_death"].dt.days

        df2 = df2.drop(columns="ingress_date")
        df2 = df2.rename(columns={"death_date":"time_id"})

        df2 = df2.groupby(["mun_id", "time_id"]).agg({"daily_deaths": "sum", "days_between_ingress_and_death": "mean"}).reset_index()

        ##Add missing dates deaths 
        df2_ = []
        last_day = df2["time_id"].max()
        for a, df_a in df2.groupby("mun_id"):
            
            first_day = df_a["time_id"].min()
            idx = pd.date_range(first_day, last_day)
            df_b = df_a.reindex(idx)
            df_b = pd.DataFrame(df_b.index)
            df_b = df_b.rename(columns={0:"time_id"})
            
            result = pd.merge(df_b, df_a, how="outer", on="time_id")
            
            df2_.append(result)
        df2_ = pd.concat(df2_, sort=False)

        df2_["mun_id"] = df2_["mun_id"].fillna(method="ffill")

        #Merge daily cases and deaths
        data = pd.merge(df1_, df2_, how="outer", on=["time_id", "mun_id"])
        data = data.sort_values(by=["time_id", "mun_id"])

        data["time_id"] = data["time_id"].astype(str).str.replace("-", "").astype(int)

        #Merge data latest report with data from each report
        data = pd.merge(data, report, how="outer", on=["time_id", "mun_id"])

        for col in ["daily_cases", "daily_hospitalized", "daily_suspect", "daily_deaths", "accum_cases_report", "new_cases_report", "accum_deaths_report", "new_deaths_report", "accum_hospitalized_report", "new_hospitalized_report", "accum_suspect_report", "new_suspect_report"]:
            data[col] = data[col].fillna(0).astype(int)

        #Add column of accumulated cases
        df_final = []
        for a, df_a in data.groupby("mun_id"):
            # create temporal df to silence "SettingWithCopyWarning"
            _df = df_a.copy()
            _df["accum_cases"] = _df["daily_cases"].cumsum()
            _df["accum_deaths"] = _df["daily_deaths"].cumsum()
            _df["accum_hospitalized"] = _df.daily_hospitalized.cumsum()
            _df["accum_suspect"] = _df.daily_suspect.cumsum()

            for i in ["daily_cases", "accum_cases", "daily_deaths", "accum_deaths", "accum_cases_report", "new_cases_report", "accum_deaths_report", "new_deaths_report"]:
                measure = "avg7_{}".format(i)
                _df[measure] = _df[i].rolling(7).mean()
                
            for j in ["daily_cases", "accum_cases", "daily_deaths", "accum_deaths", "accum_cases_report", "new_cases_report", "accum_deaths_report", "new_deaths_report"]:
                measure = "sum_last7_{}".format(j)
                _df[measure] = _df[j].rolling(7).sum()

            df_final.append(_df)
        df_final = pd.concat(df_final, sort=False)

        for k in ["sum_last7_daily_cases", "sum_last7_accum_cases", "sum_last7_daily_deaths", "sum_last7_accum_deaths", "sum_last7_accum_cases_report", "sum_last7_new_cases_report", "sum_last7_accum_deaths_report", "sum_last7_new_deaths_report"]:
            df_final[k] = df_final[k].fillna(0).astype(int)

        #Rate per 100.000 inhabitans
        df_final["population_temp"] = df_final["mun_id"].replace(dicto_mun_population)
        df_final["population"] = np.where(df_final["population_temp"] != df_final["mun_id"], df_final["population_temp"], np.nan)

        df_final["rate_daily_cases"] = (df_final["daily_cases"] / df_final["population"]) * 100000
        df_final["rate_accum_cases"] = (df_final["accum_cases"] / df_final["population"]) * 100000
        df_final["rate_new_cases_report"] = (df_final["new_cases_report"] / df_final["population"]) * 100000
        df_final["rate_accum_cases_report"] = (df_final["accum_cases_report"] / df_final["population"]) * 100000

        df_final["rate_daily_deaths"] = (df_final["daily_deaths"] / df_final["population"]) * 100000
        df_final["rate_accum_deaths"] = (df_final["accum_deaths"] / df_final["population"]) * 100000
        df_final["rate_new_deaths_report"] = (df_final["new_cases_report"] / df_final["population"]) * 100000
        df_final["rate_accum_deaths_report"] = (df_final["accum_deaths_report"] / df_final["population"]) * 100000

        df_final = df_final.drop(columns={"population", "population_temp"})
            
        #Add a column with days, being day one when at least 50 cases accumulate
        day = []
        for a, df_a in df_final.groupby("mun_id"):
            default = 0
            for row in df_a.iterrows():
                if row[1]["accum_cases"] >= 50:
                    default = default + 1

                day.append(default)
        df_final["day_from_50_cases"] = day

        #Add a column with days, being day one when at least 10 deaths accumulate
        day = []
        for a, df_a in df_final.groupby("mun_id"):
            default = 0
            for row in df_a.iterrows():
                if row[1]["accum_deaths"] >= 10:
                    default = default + 1

                day.append(default)
        df_final["day_from_10_deaths"] = day
        
        df_final["mun_id"] = df_final["mun_id"].astype(int)
        
        return df_final


class CovidStatsMunPipeline(EasyPipeline):
    @staticmethod
    def steps(params):
        db_connector = Connector.fetch("clickhouse-database", open("../conns.yaml"))

        dtypes = {
            "time_id":                          "UInt32",
            "mun_id":                           "UInt16",
            'daily_cases':                      "UInt32",
            'daily_deaths':                     "UInt32",
            'daily_hospitalized':               "UInt32",
            'daily_suspect':                    "UInt32",
            'accum_cases':                      "UInt32",
            'accum_deaths':                     "UInt32",
            'accum_hospitalized':               "UInt32",
            'accum_suspect':                    "UInt32",
            'days_between_ingress_and_death':   "Float32",
            'new_cases_report':                 "UInt32",
            'new_deaths_report':                "UInt32",
            'new_hospitalized_report':          "UInt32",
            'new_suspect_report':               "UInt32", 
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

        download_step = DownloadStep(
            connector="covid-data-mx",
            connector_path="conns.yaml",
            force=True
        )

        path = grab_parent_dir(".") + "/covid/"
        unzip_step = UnzipToFolderStep(compression="zip", target_folder_path=path)
        xform_step = TransformStep(connector=db_connector)
        load_step = LoadStep(
            "gobmx_covid_stats_mun", db_connector, if_exists="drop", 
            pk=["time_id", "mun_id"], 
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

        return [download_step, unzip_step, xform_step, load_step]

if __name__ == "__main__":
    pp = CovidStatsMunPipeline()
    pp.run({})