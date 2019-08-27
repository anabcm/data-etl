import pandas as pd
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
        df_columns = ["EDAD", "SEXO", "ENTRESIDENCIA", "MUNRESIDENCIA", "AFECPRIN", "FECHAINGRESO", "HORAINIATE", "MININIATE", "HORATERATE","MINTERATE"]

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

        # Redefining the date_id column given the 2 types of datetime format
        if df["FECHAINGRESO"][0].find(":") > 0:
            df["_YEAR"] = df["FECHAINGRESO"].apply(lambda x: str(x)[0:4])
            df["_MONTH"] = df["FECHAINGRESO"].apply(lambda x: str(x)[5:7])
            df["_DAY"] = df["FECHAINGRESO"].apply(lambda x: str(x)[8:10])
            df["date_id"] = df["_YEAR"] + df["_MONTH"] + df["_DAY"]
        else:
            df["_YEAR"] = df["FECHAINGRESO"].apply(lambda x: str(x)[6:10])
            df["_MONTH"] = df["FECHAINGRESO"].apply(lambda x: str(x)[0:2])
            df["_DAY"] = df["FECHAINGRESO"].apply(lambda x: str(x)[3:5])
            df["date_id"] = df["_YEAR"] + df["_MONTH"] + df["_DAY"]

        # Creating mun_id and count column (number of people)
        df["mun_id"] = df["ENTRESIDENCIA"] + df["MUNRESIDENCIA"]
        df["count"] = 1

        # Renaming useful columns
        df.rename(index=str, columns={"EDAD": "age", "SEXO": "sex_id", "AFECPRIN": "cie10"}, inplace=True)

        # Calculating attention time per person
        df["HORAINIATE"] = df["HORAINIATE"].astype(int)
        df["MININIATE"] = df["MININIATE"].astype(int)
        df["HORATERATE"] = df["HORATERATE"].astype(int)
        df["MINTERATE"] = df["MINTERATE"].astype(int)

        df["attention_time"] = 60 * (df["HORATERATE"] - df["HORAINIATE"]) + (df["MINTERATE"] - df["MININIATE"])

        # Droping the used columns
        list_drop = ["ENTRESIDENCIA", "MUNRESIDENCIA", "FECHAINGRESO", "HORAINIATE", "HORATERATE" , "MININIATE", "MINTERATE",
                        "_YEAR", "_MONTH", "_DAY"]
        df.drop(list_drop, axis=1, inplace=True)

        # Groupby method
        group_list = ["age", "sex_id", "cie10", "date_id", "mun_id", "attention_time"]
        df = df.groupby(group_list).sum().reset_index(col_fill="ffill")

        for item in ["age", "sex_id", "mun_id", "count", "attention_time"]:
            df[item] = df[item].astype(int)

        return df

class EmergencyPipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return [
            Parameter(label="Year", name="year", dtype=str)
        ]

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch("clickhouse-database", open("../conns.yaml"))

        dtype = {
            "age":                 "UInt8",
            "sex_id":              "UInt8",
            "cie10":               "String",
            "date_id":             "UInt32",
            "mun_id":              "UInt16",
            "attention_time":      "UInt16",
            "count":               "UInt16"
        }

        download_step = DownloadStep(
            connector="emergency-data",
            connector_path="conns.yaml"
        )
        transform_step = TransformStep()
        load_step = LoadStep(
            "dgis_emergency", db_connector, if_exists="append", pk=["mun_id", "date_id"], dtype=dtype
        )

        return [download_step, transform_step, load_step]
