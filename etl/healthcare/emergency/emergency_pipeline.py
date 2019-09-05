import pandas as pd
import numpy as np
from datetime import timedelta 
from datetime import datetime
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import DownloadStep
from bamboo_lib.steps import LoadStep


DELIMITERS = {
    "2012": ";",
    "2013": ";",
    "2014": ";",
    "2015": ",",
    "2016": ",",
    "2017": "|"
}

HEADERS = {
    "2012": None,
    "2013": None,
    "2014": None,
    "2015": 0,
    "2016": 0,
    "2017": 0
}

class TransformStep(PipelineStep):
    def run_step(self, prev, params):

        # Loading labels and df_columns given the 3 types of file format
        labels = ["ID", "CLUES", "FOLIO", "FECHAALTA", "EDAD", "CVEEDAD", "SEXO", "ENTRESIDENCIA", "MUNRESIDENCIA", "DERHAB",
                "TIPOURGENCIA", "MOTATE", "TIPOCAMA", "ENVIADOA", "MP", "AFECPRIN", "IRA", "PLANEDA", "SOBRESEDA",
                "FECHAINGRESO", "HORASESTANCIA", "MES_ESTADISTICO", "HORAINIATE", "MININIATE", "HORATERATE", "MINTERATE"]
        df_columns = ["EDAD", "SEXO", "DERHAB", "FECHAALTA", "ENTRESIDENCIA", "MUNRESIDENCIA", "AFECPRIN", "FECHAINGRESO", "HORAINIATE", "MININIATE", "HORATERATE","MINTERATE"]
        # Reading step, testing each format type for emergency files
        if int(params["year"]) in list(range(2012, 2015)):
            df = pd.read_csv(prev, index_col=None, header=HEADERS[params["year"]], sep=DELIMITERS[params["year"]], encoding="latin-1", dtype=str, chunksize=10**4)
            df = pd.concat(df)
            df.columns = labels
            df = df[df_columns]

        elif int(params["year"]) in list(range(2015, 2017)):
            df = pd.read_csv(prev, index_col=None, header=HEADERS[params["year"]], sep=DELIMITERS[params["year"]], encoding="latin-1", dtype=str, chunksize=10**4)
            df = pd.concat(df)
            df = df[df_columns]

        else:
            df = pd.read_csv(prev, index_col=None, header=HEADERS[params["year"]], sep=DELIMITERS[params["year"]], encoding="latin-1", dtype=str, chunksize=10**4)
            df = pd.concat(df)
            df = df[df_columns]

        # Deleting empty odds spaces in the time and geography columns
        timers = ["HORAINIATE", "MININIATE", "HORATERATE", "MINTERATE"]
        for item in timers:
            df[item] = df[item].str.strip()
            df[item].replace("99", "00", inplace = True)

        df["ENTRESIDENCIA"] = df["ENTRESIDENCIA"].str.strip()
        df["MUNRESIDENCIA"] = df["MUNRESIDENCIA"].str.strip()

        # Redefining the date_id column given the 2 types of datetime format
        if df["FECHAINGRESO"][0].find(":") > 0:
            df["_YEAR"] = df["FECHAINGRESO"].apply(lambda x: str(x)[0:4])
            # Fixed issue with 2017 year file
            df.loc[df["_YEAR"] == "2520", "_YEAR"] ="0000"
            df["_MONTH"] = df["FECHAINGRESO"].apply(lambda x: str(x)[5:7])
            df["_DAY"] = df["FECHAINGRESO"].apply(lambda x: str(x)[8:10])
            df["date_id"] = df["_YEAR"] + df["_MONTH"] + df["_DAY"]
        else:
            dates = pd.to_datetime(df["FECHAINGRESO"]).dt.date.astype("str")
            df["_YEAR"] = dates.apply(lambda x: str(x)[0:4])
            df["_MONTH"] = dates.apply(lambda x: str(x)[5:7])
            df["_DAY"] = dates.apply(lambda x: str(x)[8:10])
            df["date_id"] = df["_YEAR"] + df["_MONTH"] + df["_DAY"]

        # No date of admission issue
        df.drop(df.loc[df["date_id"] == "nan"].index, inplace=True)

        # Renaming useful columns
        df.rename(index=str, columns={"EDAD": "age", "SEXO": "sex_id", "AFECPRIN": "cie10", "DERHAB": "social_security"}, inplace=True)

        # Replacing issues with 2017 file
        if int(params["year"]) == 2017:
            df["FECHAINGRESO"] = df["FECHAINGRESO"].str.replace("2520", "2017")
        # df.drop(df.loc[df["FECHAINGRESO"].str.contains("2520")].index, inplace = True)

        # Calculating attention time per person
        df["datetime_leaving"] = pd.to_datetime(df["FECHAALTA"]).dt.date.astype("str") + " " + df["HORATERATE"].astype("str").str.zfill(2) + ":" + df["MINTERATE"].astype("str").str.zfill(2) + ":" + "00"
        df["datetime_admission"] = pd.to_datetime(df["FECHAINGRESO"]).dt.date.astype("str") + " " + df["HORAINIATE"].astype("str").str.zfill(2) + ":" + df["MININIATE"].astype("str").str.zfill(2) + ":" + "00"

        df["datetime_leaving"] = pd.to_datetime(df["datetime_leaving"], format='%Y-%m-%d %H:%M:%S', errors="coerce")
        df["datetime_admission"] = pd.to_datetime(df["datetime_admission"], format = '%Y-%m-%d %H:%M:%S', errors="coerce")
            
        # attention_time in hours [Some people has NaN values given that they dont have date of admission]
        df["attention_time"] = df["datetime_leaving"] - df["datetime_admission"]
        df["attention_time"] = df["attention_time"] / np.timedelta64(1,"m")

        # Replacing issues with 2016 file
        df["MUNRESIDENCIA"].replace("K29", "029", inplace = True)
        df["MUNRESIDENCIA"].replace(pd.np.nan, "999", inplace = True)
        df["ENTRESIDENCIA"].replace(pd.np.nan, "99", inplace = True)

        # Creating mun_id and count column (number of people)
        df["mun_id"] = df["ENTRESIDENCIA"].astype("str").str.zfill(2) + df["MUNRESIDENCIA"].astype("str").str.zfill(3)
        df["count"] = 1

        # Fixing issues with switched dates and adding column related to exceding time in urgencies
        df["attention_time"] = df["attention_time"].abs()
        df.loc[(df["attention_time"] > 720), "over_time"] = 0
        df.loc[(df["attention_time"] < 720), "over_time"] = 1

        # Droping the used columns
        list_drop = ["ENTRESIDENCIA", "MUNRESIDENCIA", "FECHAINGRESO", "HORAINIATE", "HORATERATE" , "MININIATE", "MINTERATE",
                    "FECHAALTA", "_YEAR", "_MONTH", "_DAY", "datetime_leaving", "datetime_admission"]

        df.drop(list_drop, axis=1, inplace=True)

        # Groupby method
        group_list = ["age", "sex_id", "social_security", "cie10", "date_id", "mun_id", "attention_time", "over_time"]
        df = df.groupby(group_list).sum().reset_index(col_fill="ffill")

        for item in ["age", "sex_id", "mun_id", "count", "date_id"]:
            df[item] = df[item].astype(int)

        df["social_security"] = df["social_security"].apply(lambda x: x.strip()).replace({"G": pd.np.nan, "P": "10", "3":"5", "4": "5", "6": "4", "7": "6", "2": "3", "1": "2", "8": "1", "0": "8", "9": "0"})

        for item in ["social_security", "attention_time"]:
            df[item] = df[item].astype(float)

        return df

class EmergencyPipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return [
            Parameter(label="Year", name="year", dtype=str)
        ]

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch("clickhouse-database", open("../../conns.yaml"))

        dtype = {
            "age":                 "UInt8",
            "sex_id":              "UInt8",
            "social_security":     "UInt8",
            "cie10":               "String",
            "date_id":             "UInt32",
            "mun_id":              "UInt16",
            "attention_time":      "UInt16",
            "count":               "UInt16",
            "over_time":           "UInt8"
        }

        download_step = DownloadStep(
            connector="emergency-data",
            connector_path="conns.yaml"
        )
        transform_step = TransformStep()
        load_step = LoadStep(
            "dgis_emergency", db_connector, if_exists="append", pk=["sex_id", "mun_id"], dtype=dtype,
            nullable_list=["date_id", "social_security"]
        )

        return [download_step, transform_step, load_step]
