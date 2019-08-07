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

        df = df.drop(columns=[
          "Total de educandos atendidos en alfabetización",
          "Total de educandos atendidos en nivel inicial",
          "Total de educandos atendidos en nivel intermedio (primaria)",
          "Total de educandos atendidos en nivel avanzado (secundaria)",
          "Total de educandos alfabetizados",
          "Total de educandos que concluyeron el nivel inicial",
          "Total de educandos que concluyeron el nivel intermedio (primaria)",
          "Total de educandos que concluyeron el nivel avanzado (secundaria)",
          "Total de educandos incorporados a alfabetización",
          "Total de educandos inscritos",
          "Total de Educandos registrados"
        ])

        df = df.rename(columns={
          "Fecha de corte": "month_id",
          "Entidad Federativa": "ent_id",
          "Educandos atendidos en alfabetización en la vertiente hispanohablante": "student_served_sp_speaker",
          "Educandos atendidos en alfabetización en la vertiente indígena": "student_served_indigenous",

          "Educandos atendidos en nivel inicial en la vertiente hispanohablante": "student_served_initial_sp_speaker",
          "Educandos atendidos en nivel inicial en la vertiente indígena": "student_served_initial_indigenous",

          "Educandos atendidos en nivel intermedio (primaria) en la vertiente hispanohablante": "student_served_intermediate_sp_speaker",
          "Educandos atendidos en nivel intermedio (primaria) en la vertiente indígena": "student_served_intermediate_indigenous",

          "Educandos atendidos en nivel avanzado (secundaria) en la vertiente hispanohablante": "student_served_advanced_sp_speaker",
          "Educandos atendidos en nivel avanzado (secundaria) en la vertiente indígena": "student_served_advanced_indigenous",

          "Educandos alfabetizados en la vertiente hispanohablante": "student_literate_sp_speaker",
          "Educandos alfabetizados en la vertiente indígena": "student_literate_indigenous",

          "Educandos que concluyeron nivel inicial en la vertiente hispanohablante": "student_literate_initial_sp_speaker",
          "Educandos que concluyeron el nivel inicial en la vertiente indígena": "student_literate_initial_indigenous",

          "Educandos que concluyeron nivel intermedio (primaria) en la vertiente hispanohablante": "student_literate_intermediate_sp_speaker",
          "Educandos que concluyeron el nivel intermedio (primaria) en la vertiente indígena": "student_literate_intermediate_indigenous",

          "Educandos que concluyeron nivel avanzado (secundaria)  en la vertiente hispanohablante": "student_literate_advanced_sp_speaker",
          "Educandos que concluyeron el nivel avanzado (secundaria) en la vertiente indígena": "student_literate_advanced_indigenous",

          "Total de educandos incorporados a alfabetización en la vertiente hispanohablante": "student_incorporated_sp_speaker",
          "Total de educandos que incorporados a alfabetización en la vertiente indígena": "student_incorporated_indigenous",

          "Educandos inscritos en la vertiente hispanohablante": "student_inscribed_sp_speaker",
          "Educandos inscritos en la vertiente indígena": "student_inscribed_indigenous",

          "Educandos registrados en la vertiente hispanohablante": "student_registered_sp_speaker",
          "Educandos registrados en la vertiente indígena": "student_registered_indigenous",

          "Total de Asesores activos": "n_advisors",
          "Total de Técnicos Docente activos": "n_tech_professors",

          "Total de Coordinaciones de zona en operación": "n_zone_coordinations",
          "Total de Plazas comunitarias en operación": "n_community_places",

          "Total de Círculos de estudio en operación": "n_study_zones",
          "Total de Puntos de encuentro en operación": "n_meeting_zones"
        })

        iterator = [
          "student_served", "student_served_initial", "student_served_intermediate", "student_served_advanced",
          "student_literate", "student_literate_initial", "student_literate_intermediate", "student_literate_advanced",
          "student_incorporated", "student_inscribed", "student_registered"
        ]

        frames = []
        for i in iterator:
            sp_speaker_label = "{}_sp_speaker".format(i)
            indigenous_label = "{}_indigenous".format(i)
            item = df[["month_id", "ent_id", sp_speaker_label, indigenous_label]].melt(
                id_vars=["month_id", "ent_id"], 
                var_name="origin_id",
                value_vars=[sp_speaker_label, indigenous_label],
                value_name=i
            ).replace(
                {sp_speaker_label: 1, indigenous_label: 2}
            )
            item["concat_id"] = item["ent_id"].astype(str) + item["month_id"].astype(str) + item["origin_id"].astype(str)
            frames.append(item.set_index("concat_id"))

        output = pd.concat(frames, axis=1, join="inner")
        output = output.loc[:,~output.columns.duplicated()]
        output = output.drop(columns=["concat_id"])

        return output

class INEAPipeline(EasyPipeline):
    @staticmethod
    def steps(params):
        db_connector = Connector.fetch("clickhouse-database", open("../conns.yaml"))
        dtype = {
            "month_id":     "UInt16",
            "ent_id":       "UInt8",
            "origin_id":    "UInt8"
        }

        extract_step = ExtractStep()
        load_step = LoadStep(
            "inea_adult_education_origin", db_connector, if_exists="drop", dtype=dtype,
            pk=["ent_id", "month_id", "origin_id"]
        )

        return [extract_step, load_step]

