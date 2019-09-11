import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import DownloadStep
from bamboo_lib.steps import LoadStep


class TransformStep(PipelineStep):
    def run_step(self, prev, params):

        df_labels = "https://docs.google.com/spreadsheets/d/e/2PACX-1vR6a5TEhnjteU3aa96H7iG4OoNAMkCgsQJ52HPbycoStRBIer66JnfsSbS5tbmlkdQ6jwn2dp8xBb0U/pub?output=xlsx"
        df = pd.read_csv(prev, encoding="latin-1")

        list_drop = ["Clave_Ent", "Entidad", "Municipio", "Bien jurídico afectado", "Tipo de delito"]
        df.drop(list_drop, axis=1, inplace=True)

        dict_months = {
          "Enero": "1",
          "Febrero": "2",
          "Marzo": "3",
          "Abril": "4",
          "Mayo": "5",
          "Junio": "6",
          "Julio": "7",
          "Agosto": "8",
          "Septiembre": "9",
          "Octubre": "10",
          "Noviembre": "11",
          "Diciembre": "12"}

        df.rename(index=str, columns={"Año": "year",
                                      "Cve. Municipio": "mun_id",
                                      "Subtipo de delito": "crime_subtype",
                                      "Modalidad": "crime_modality"}, inplace=True)

        df.rename(columns=dict(dict_months), inplace=True)

        # Melt step in order to get file in tidy data format
        df = pd.melt(df.copy(), id_vars = ["year", "mun_id", "crime_subtype", "crime_modality"], value_vars = list(dict_months.values()))

        df.dropna(axis = 0, how = "any", inplace = True)

        df["month_id"] = df["year"].astype(str) + df["variable"].astype(str).str.zfill(2)

        df.drop(["variable", "year"], axis=1, inplace=True)

        df_l = pd.read_excel(df_labels, "crime_subtype")
        df["crime_subtype"] = df["crime_subtype"].replace(dict(zip(df_l.crime_subtype_es, df_l.crime_subtype_id)))

        df_l = pd.read_excel(df_labels, "crime_modality")
        df["crime_modality"] = df["crime_modality"].replace(dict(zip(df_l.crime_modality_es, df_l.crime_modality_id)))

        df.rename(index=str, columns={"crime_subtype": "crime_subtype_id",
                                      "crime_modality": "crime_modality_id"}, inplace=True)

        # Transforming certains columns into int values
        for col in ["mun_id", "crime_subtype_id", "crime_modality_id", "value", "month_id"]:
            df[col] = df[col].astype(int)

        return df

class CrimesPipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return [
        ]

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch("clickhouse-database", open("../conns.yaml"))

        dtype = {
            "mun_id":               "UInt16",
            "crime_subtype_id":     "UInt32",
            "crime_modality_id":    "UInt8",
            "value":                "UInt16",
            "month_id":             "UInt32
        }

        download_step = DownloadStep(
            connector="crimes-data",
            connector_path="conns.yaml"
        )
        transform_step = TransformStep()
        load_step = LoadStep(
            "sesnsp_crimes", db_connector, if_exists="drop",
            pk=["mun_id", "month_id", "crime_modality_id", "crime_subtype_id"], dtype=dtype
        )

        return [download_step, transform_step, load_step]