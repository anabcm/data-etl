import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import DownloadStep
from bamboo_lib.steps import LoadStep


class ExtractStep(PipelineStep):
    def run_step(self, prev, params):
        df = pd.read_excel("http://www.inea.gob.mx/images/documentos/datos/INEA_Numeros.xlsx")
        df.columns = df.columns.str.strip()

        ent_ids = {
          "AGUASCALIENTES": 1,
          "BAJA CALIFORNIA": 2,
          "BAJA CALIFORNIA SUR": 3,
          "CAMPECHE": 4,
          "COAHUILA": 5,
          "COLIMA": 6,
          "CHIAPAS": 7,
          "CHIHUAHUA": 8,
          "DISTRITO FEDERAL": 9,
          "DURANGO": 10,
          "GUANAJUATO": 11,
          "GUERRERO": 12,
          "HIDALGO": 13,
          "JALISCO": 14,
          "MEXICO": 15,
          "MICHOACAN": 16,
          "MORELOS": 17,
          "NAYARIT": 18,
          "NUEVO LEON": 19,
          "OAXACA": 20,
          "PUEBLA": 21,
          "QUERETARO": 22,
          "QUINTANA ROO": 23,
          "SAN LUIS POTOSI": 24,
          "SINALOA": 25,
          "SONORA": 26,
          "TABASCO": 27,
          "TAMAULIPAS": 28,
          "TLAXCALA": 29,
          "VERACRUZ": 30,
          "YUCATAN": 31,
          "ZACATECAS": 32
        }

        df["Entidad Federativa"] = df["Entidad Federativa"].str.strip().replace(ent_ids)
        df["Fecha de corte"] = df["Fecha de corte"].map(lambda x: x.strftime("%Y%m")).astype(int)

        df = df.rename(columns={
          "Fecha de corte": "month_id",
          "Entidad Federativa": "ent_id",

          "Total de Asesores activos": "n_advisors",
          "Total de Técnicos Docente activos": "n_tech_professors",

          "Total de Coordinaciones de zona en operación": "n_zone_coordinations",
          "Total de Plazas comunitarias en operación": "n_community_places",

          "Total de Círculos de estudio en operación": "n_study_zones",
          "Total de Puntos de encuentro en operación": "n_meeting_zones"
        })

        df = df[["month_id", "ent_id", "n_advisors", "n_tech_professors", "n_zone_coordinations", "n_community_places", "n_study_zones", "n_meeting_zones"]]

        return df


class INEAStatsPipeline(EasyPipeline):
    @staticmethod
    def steps(params):
        db_connector = Connector.fetch("clickhouse-database", open("../conns.yaml"))
        dtype = {
            "month_id":     "UInt16",
            "ent_id":       "UInt8"
        }

        extract_step = ExtractStep()
        load_step = LoadStep(
            "inea_adult_education_stats", db_connector, if_exists="drop", dtype=dtype,
            pk=["ent_id", "month_id"]
        )

        return [extract_step, load_step]

