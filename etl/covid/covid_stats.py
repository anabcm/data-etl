
import glob
import numpy as np
import os
import pandas as pd
import requests

from bamboo_lib.helpers import grab_parent_dir
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, PipelineStep, Parameter
from bamboo_lib.steps import DownloadStep, LoadStep, UnzipToFolderStep
from shared import rename_columns, rename_countries
from helpers import norm


class TransformStep(PipelineStep):
    def run_step(self, prev, params):

        data = sorted(glob.glob("*.csv"))

        if params.get("file_path"):
            df = pd.read_csv(params.get("file_path"), encoding="latin-1")
        else:
            df = pd.read_csv(data[-1], encoding="latin-1")
        
        r = requests.get("https://api.datamexico.org/tesseract/data?Year=2015&cube=inegi_population&drilldowns=State&measures=Population")
        data_json = r.json()
        states_data = pd.DataFrame(data_json["data"])
        dicto_states_population = dict(zip(states_data["State ID"], states_data["Population"]))

        df.columns = [x.strip().lower().replace(" ", "_") for x in df.columns]

        df1 = df[["entidad_res", "fecha_ingreso", "resultado"]]
        df1 = df1[df1["resultado"] == 1]
        df1 = df1.rename(columns={"entidad_res":"ent_id", "fecha_ingreso":"date", "resultado":"daily_cases"})

        df1 = df1.groupby(["ent_id","date"]).sum().reset_index()
        df1["date"] = pd.to_datetime(df1["date"])

        ##Add missing dates daily cases
        df1_ = []
        for a, df_a in df1.groupby("ent_id"):
            
            first_day = df_a.date.min()
            last_day = df_a.date.max()
            idx = pd.date_range(first_day, last_day)
            df_b = df_a.reindex(idx)
            df_b = pd.DataFrame(df_b.index)
            df_b = df_b.rename(columns={0:"date"})
            
            result = pd.merge(df_b, df_a, how="outer", on="date")
            
            df1_.append(result)
        df1_ = pd.concat(df1_, sort=False)

        df1_["ent_id"] = df1_["ent_id"].fillna(method="ffill")

        #Deaths
        df2 = df[["entidad_res","fecha_ingreso", "fecha_def", "resultado"]]

        df2 = df2.rename(columns={"entidad_res":"ent_id", "fecha_ingreso":"ingress_date", "fecha_def":"death_date", "resultado":"daily_deaths"})

        df2 = df2[df2["daily_deaths"] == 1]
        df2 = df2[df2["death_date"]!= "9999-99-99"]

        for i in ["ingress_date", "death_date"]:
            df2[i] = pd.to_datetime(df2[i])
            
        df2["days_between_ingress_and_death"] = df2["death_date"] - df2["ingress_date"]
        df2["days_between_ingress_and_death"] = df2["days_between_ingress_and_death"].dt.days

        df2 = df2.drop(columns="ingress_date")
        df2 = df2.rename(columns={"death_date":"date"})

        df2 = df2.groupby(["ent_id","date"]).agg({"daily_deaths":"sum", "days_between_ingress_and_death": "mean"}).reset_index()

        #Merge daily cases and deaths
        data = pd.merge(df1_, df2, how="outer", on=["date", "ent_id"])

        for col in ["ent_id", "daily_cases", "daily_deaths"]:
            data[col] = data[col].fillna(0).astype(int)

        #Add column of accumulated cases
        df_final = []
        for a, df_a in data.groupby("ent_id"):
            df_a["accum_cases"] = df_a.daily_cases.cumsum()
            df_a["accum_deaths"] = df_a.daily_deaths.cumsum()
            df_final.append(df_a)
        df_final = pd.concat(df_final, sort=False)

        #Rate per 100.000 inhabitans
        df_final["population"] = df_final["ent_id"].replace(dicto_states_population)

        df_final["rate_daily_cases"] = (df_final["daily_cases"] / df_final["population"]) * 100000
        df_final["rate_accum_cases"] = (df_final["accum_cases"] / df_final["population"]) * 100000

        df_final["rate_daily_deaths"] = (df_final["daily_deaths"] / df_final["population"]) * 100000
        df_final["rate_accum_deaths"] = (df_final["accum_deaths"] / df_final["population"]) * 100000

        df_final = df_final.drop(columns="population")

        #Add moving average every 7 days
        days = 7
        for i in ["daily_cases", "accum_cases", "daily_deaths", "accum_deaths"]:
            measure = "avg_7_days_{}".format(i)
            df = df_final.groupby("ent_id").apply(lambda x: x.set_index("date").resample("1D").first())
            df_temp = df.groupby(level=0)[i].apply(lambda x: x.shift().rolling(window=days).mean()).reset_index(name=measure)
            df_final = pd.merge(df_final, df_temp, on=["ent_id", "date"], how="left")
            
        #Add column with day from first case and death
        for col in ["accum_cases", "accum_deaths"]:
            measure = "{}_day".format(col)
            day = []
            for a, df_a in df_final.groupby("ent_id"):
                default = 0
                for row in df_a.iterrows():
                    if row[1][col] != 0:
                        default = default + 1

                    day.append(default)
            df_final[measure] = day 
            
        df_final = df_final.rename(columns={"accum_cases_day": "cases_day", "accum_deaths_day": "deaths_day"})

        df_final["date"] = df_final["date"].astype(str).str.replace("-", "").astype(int)
      
        return df_final


class CovidStatsPipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return [
            Parameter(label='file_path', name='file_path', dtype=str)
        ]

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))

        dtypes = {
            'date':                             'UInt32',
            'ent_id':                           'UInt8',
            'daily_cases':                      'UInt32',
            'daily_deaths':                     'UInt32',
            'days_between_ingress_and_death':   'Float32',
            'accum_cases':                      'UInt32',
            'accum_deaths':                     'UInt32',
            'rate_daily_cases':                 'Float32',
            'rate_accum_cases':                 'Float32',
            'rate_daily_deaths':                'Float32',
            'rate_accum_deaths':                'Float32',
            'avg_7_days_daily_cases':           'Float32',
            'avg_7_days_accum_cases':           'Float32',
            'avg_7_days_daily_deaths':          'Float32',
            'avg_7_days_accum_deaths':          'Float32',
            'cases_day':                        'UInt16',
            'deaths_day':                       'UInt16',
        }

        download_step = DownloadStep(
            connector='covid-data-mx',
            connector_path='conns.yaml',
            force=True
        )

        path = grab_parent_dir('.') + '/covid/'
        unzip_step = UnzipToFolderStep(compression='zip', target_folder_path=path)
        xform_step = TransformStep()
        load_step = LoadStep(
            'gobmx_covid_stats', db_connector, if_exists='drop', 
            pk=['date', 'ent_id'], 
            nullable_list=['days_between_ingress_and_death', 'avg_7_days_daily_cases', 'avg_7_days_accum_cases', 'avg_7_days_daily_deaths', 'avg_7_days_accum_deaths'], 
            dtype=dtypes
        )

        if params.get('file_path'):
            return [xform_step, load_step]
        else:
            return [download_step, unzip_step, xform_step, load_step]

if __name__ == '__main__':
    pp = CovidStatsPipeline()
    pp.run({})