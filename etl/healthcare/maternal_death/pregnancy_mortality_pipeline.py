import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import DownloadStep
from bamboo_lib.steps import LoadStep


class TransformStep(PipelineStep):
    def run_step(self, prev, params):

        # Loading labels and df_cols
        df_cols = ["Edad cumplida", "Estado conyugal", "Entidad de residencia", "Municipio de residencia",
                  "Ocupación habitual", "Escolaridad", "Derechohabiencia", "Sitio donde ocurrio la defunción", "Año de la defunción",
                  "Entidad de ocurrencia", "Municipio de ocurrencia", "Causa CIE a cuatro dígitos", "Año de registro"]

        excel_url = "https://docs.google.com/spreadsheets/d/e/2PACX-1vTSLookaSnn_N-gKf-WgVIGIeQn54iWTGJq4F5NtddlNrDeb8-Z7kRadjVbVVmyoerX5miH19UvdzlM/pub?output=xlsx"
        df_labels = pd.ExcelFile(excel_url)

        # Reading the csv file
        df = pd.read_csv(prev, index_col=None, header=0, encoding="utf-8", dtype=str, chunksize=10**4, usecols=df_cols)
        df = pd.concat(df)

        # Creating the municipality ID, one for residence/housing and one for the place of death
        df["mun_residence_id"] = df["Entidad de residencia"] + df["Municipio de residencia"]
        df["mun_happening_id"] = df["Entidad de ocurrencia"] + df["Municipio de ocurrencia"]

        # Droping the used columns
        list_drop = ["Entidad de residencia", "Municipio de residencia", "Entidad de ocurrencia", "Municipio de ocurrencia"]
        df.drop(list_drop, axis=1, inplace=True)

        # Translating columns to english
        col_names = pd.read_excel(df_labels, "renames")
        df.rename(columns = dict(zip(col_names.previous, col_names.new)), inplace=True)

        # Creating mun_id and count column (number of people)
        df["count"] = 1

        # Groupby method
        group_list = ["age", "marital_status", "occupation", "academic_degree", "social_security", "medical_center",
                      "year_decease", "cie10", "year_of_register", "mun_residence_id", "mun_happening_id"]
        df = df.groupby(group_list).sum().reset_index(col_fill="ffill")

        # Transforming to int type columns
        for item in ["age", "marital_status", "occupation", "academic_degree", "social_security", "medical_center",
                      "year_decease","year_of_register", "mun_residence_id", "mun_happening_id", "count"]:
            df[item] = df[item].astype(int)

        return df

class PregnancyMortalityPipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return [
            Parameter(label="Year", name="year", dtype=str)
        ]

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch("clickhouse-database", open("../../conns.yaml"))

        dtype = {
            "age":                  "UInt8",
            "marital_status":       "UInt8",
            "occupation":           "UInt8",
            "academic_degree":      "UInt8",
            "social_security":      "UInt8",
            "medical_center":       "UInt8",
            "year_decease":         "UInt16",
            "cie10":                "String",
            "year_of_register":     "UInt16",
            "mun_residence_id":     "UInt16",
            "mun_happening_id":     "UInt16",
            "count":                "UInt8"
        }

        download_step = DownloadStep(
            connector="pregnancy-mortality-data",
            connector_path="conns.yaml"
        )
        transform_step = TransformStep()
        load_step = LoadStep(
            "dgis_pregnancy_mortality", db_connector, if_exists="drop", pk=["mun_residence_id", "year_decease"], dtype=dtype
        )

        return [download_step, transform_step, load_step]
