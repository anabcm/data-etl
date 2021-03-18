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
        df_cols = {x: x.upper() for x in df_cols}

        df_labels = pd.ExcelFile(prev[1])

        # Reading the csv file
        df = pd.read_csv(prev[0], index_col=None, header=0, encoding="latin-1", dtype=str, chunksize=10**4, usecols=list(df_cols.values()))
        df = pd.concat(df)
        df.rename(columns=dict(zip(list(df_cols.values()), list(df_cols.keys()))), inplace=True)

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

        # Setting academic_degree and social_security ids to match the previous datasets
        filling = ["academic_degree", "social_security"]
        for sheet in filling:
            df_l = pd.read_excel(df_labels, sheet)
            df[sheet] = df[sheet].astype(float)
            df[sheet] = df[sheet].replace(dict(zip(df_l.prev_id, df_l.id)))

        # Transforming to int type columns
        for item in ["age", "marital_status", "occupation", "academic_degree", "social_security", "medical_center",
                      "year_decease","year_of_register", "mun_residence_id", "mun_happening_id", "count"]:
            df[item] = df[item].astype(int)

        return df

class PregnancyMortalityPipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return []

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
            connector=["pregnancy-mortality-data", "dim-pregnancy-mortality"],
            connector_path="conns.yaml",
            force=True
        )
        transform_step = TransformStep()
        load_step = LoadStep(
            "dgis_pregnancy_mortality", db_connector, if_exists="drop", pk=["mun_residence_id", "year_decease"], dtype=dtype
        )

        return [download_step, transform_step, load_step]