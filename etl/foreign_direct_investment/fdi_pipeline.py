import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import DownloadStep
from bamboo_lib.steps import LoadStep


class TransformStep(PipelineStep):
    def run_step(self, prev, params):

        df = pd.read_excel(prev[0], sheet_name="Tabla Data con Rama", header=1)

        df = df.loc[df["Año de materialización"] != "Total general"].copy()
        df["quarter_id"] = (df["Año de materialización"].astype(int).astype(str) + df["Trimestre de materialización"].astype(int).astype(str)).astype(int)
        columns = {
            "Inversión Genérica": "generic_investment",
            "País de Origen": "origin_id",
            "Rama": "area_id",
            "Entidad federativa": "ent_id",
            "Suma de Monto en millones": "value_million",
            "Recuento distinto de Expediente": "count"
        }
        df = df.rename(columns=columns)

        df_labels = pd.ExcelFile(prev[1])
        df_labels = pd.read_excel(df_labels, sheet_name="FDI")

        df["origin_id"] = df["origin_id"].replace(dict(zip(df_labels.name, df_labels.id)))

        generic_investment = {
            "Cuentas entre compañías": 1,
            "Nuevas inversiones": 2,
            "Reinversión de utilidades": 3
        }

        df["generic_investment"] = df["generic_investment"].replace(generic_investment)

        ent_ids = {
            "Aguascalientes": 1, "Baja California": 2, "Baja California Sur": 3, "Campeche": 4,
            "Coahuila de Zaragoza": 5, "Colima": 6, "Chiapas": 7, "Chihuahua": 8,
            "Ciudad de México": 9, "Durango": 10, "Guanajuato": 11, "Guerrero": 12,
            "Hidalgo": 13, "Jalisco": 14, "Estado de México": 15, "Michoacán de Ocampo": 16,
            "Morelos": 17, "Nayarit": 18, "Nuevo León": 19, "Oaxaca": 20,
            "Puebla": 21, "Querétaro": 22, "Quintana Roo": 23, "San Luis Potosí": 24,
            "Sinaloa": 25, "Sonora": 26, "Tabasco": 27, "Tamaulipas": 28,
            "Tlaxcala": 29, "Veracruz de Ignacio de la Llave": 30, "Yucatán": 31, "Zacatecas": 32
        }
        df["ent_id"] = df["ent_id"].replace(ent_ids)

        df = df[list(columns.values()) + ["quarter_id"]]

        for col in ["generic_investment", "area_id", "ent_id", "count"]:
            df[col] = df[col].astype(int)

        df["value_million"] = df["value_million"].astype(float)

        return df

class FDIPipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return []

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch("clickhouse-database", open("../conns.yaml"))
        dtype = {
            "generic_investment":   "UInt8",
            "origin_id":            "String",
            "area_id":              "String",
            "ent_id":               "UInt8",
            "value_million":        "Float32",
            "count":                "UInt32",
            "quarter_id":           "UInt16"
        }

        download_step = DownloadStep(
            connector=["fdi-data", "fdi-countries"],
            connector_path="conns.yaml"
        )
        transform_step = TransformStep()
        load_step = LoadStep(
            "economy_fdi", db_connector, if_exists="drop", 
            pk=["origin_id", "ent_id", "quarter_id", "generic_investment"], dtype=dtype
        )

        return [download_step, transform_step, load_step]
