import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import DownloadStep
from bamboo_lib.steps import LoadStep


class TransformStep(PipelineStep):
    def run_step(self, prev, params):

        # Loading labels and df_columns given the 3 types of file format
        labels = ["ID", "CLUES", "FOLIO", "FECHAALTA", "EDAD", "CVEEDAD", "SEXO", "ENTRESIDENCIA", "MUNRESIDENCIA", "DERHAB",
                "TIPOURGENCIA", "MOTATE", "TIPOCAMA", "ENVIADOA", "MP", "AFECPRIN", "IRA", "PLANEDA", "SOBRESEDA",
                "FECHAINGRESO", "HORASESTANCIA", "MES_ESTADISTICO", "HORAINIATE", "MININIATE", "HORATERATE", "MINTERATE"]
        df_columns = ["EDAD", "SEXO", "ENTRESIDENCIA", "MUNRESIDENCIA", "AFECPRIN", "FECHAINGRESO", "HORAINIATE", "MININIATE", "HORATERATE","MINTERATE"]

        # Reading step for emergency files

        try:
            df = pd.read_csv(prev, index_col=None, header=0, encoding="latin-1", dtype=str, usecols=df_columns)
        except:
            df = pd.read_csv(prev, index_col=None, header=0, encoding="latin-1", dtype=str, usecols=df_columns, sep= "|")

        if (len(df.columns) == 1):
            df = pd.read_csv(prev, index_col=None, header=0, encoding="latin-1", dtype=str, names = ["PINPOINT"])
            pivote = df["PINPOINT"].str.split(";", expand=True)
            pivote.columns = labels
            df = pivote
        else:
            pass

        df = df[df_columns]
        # First cleaning step to remove '"' from the columns 
        if df["EDAD"][0].find('"') == 0:
          for item in df.columns:
            df[item] = df[item].map(lambda x: x.lstrip('"').rstrip('"')) 
        else: 
          pass

        # Redefining the datetime column given the 2 types of datetime format
        if df["FECHAINGRESO"][0].find(":") > 0:
            df["FECHAINGRESO"] = df["FECHAINGRESO"].map(lambda x: str(x)[:-9])
            date = df["FECHAINGRESO"].str.split("-", expand=True)
            df["hora_id"] = df["HORAINIATE"] + df["MININIATE"]
            df["date_id"] = date[0] + date[1] + date[2] + df["hora_id"]
        else:
            date = df["FECHAINGRESO"].str.split("/", expand=True)
            df["hora_id"] = df["HORAINIATE"] + df["MININIATE"]
            df["date_id"] = date[2] + date[1] + date[0] + df["hora_id"]

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
        list_drop = ["ENTRESIDENCIA", "MUNRESIDENCIA", "FECHAINGRESO", "hora_id", "HORAINIATE", "HORATERATE" , "MININIATE", "MINTERATE"]
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
            "date_id":             "UInt64",
            "mun_id":              "UInt16",
            "attention_time":      "UInt16",
            "count":               "UInt16"
        }

        download_step = DownloadStep(
            connector=["emergency-data"],
            connector_path="conns.yaml"
        )
        transform_step = TransformStep()
        load_step = LoadStep(
            "dgis_emergency", db_connector, if_exists="append", pk=["mun_id", "date_id"], dtype=dtype
        )

        return [download_step, transform_step, load_step]
