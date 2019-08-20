import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import DownloadStep
from bamboo_lib.steps import LoadStep


class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        excel = pd.ExcelFile(prev)
        df = pd.read_excel(excel, "Estados", header=1)

        df = df[~df["Entidad federativa"].isna()].copy()
        df = df[df["Clave de la entidad"] != "00"]

        headers = [
            "ent_id", "population", "population_illiterate", "population_6_14_school", "population_15_incomplete_school", 
            "no_health_services", "dirt_floor", "no_toilet", "no_water_supply_network", "no_sewer_system", 
            "no_electrical_energy", "no_washing_machine", "no_fridge", "social_lag_index", "social_lag_degree"
        ]

        df_2000 = df[["Clave de la entidad", "Población total", "Indicadores de rezago social (porcentaje)"] + ["Unnamed: {}".format(10 + 4*i) for i in range(10)] + ["Índice de rezago social", "Grado de rezago social"]].copy()
        df_2000.columns = headers
        df_2000["year"] = 2000

        df_2005 = df[["Clave de la entidad"] + ["Unnamed: {}".format(3 + 4*i) for i in range(14)]].copy()
        df_2005.columns = headers
        df_2005["year"] = 2005

        df_2010 = df[["Clave de la entidad"] + ["Unnamed: {}".format(4 + 4*i) for i in range(14)]].copy()
        df_2010.columns = headers
        df_2010["year"] = 2010

        df_2015 = df[["Clave de la entidad"] + ["Unnamed: {}".format(5 + 4*i) for i in range(14)]].copy()
        df_2015.columns = headers
        df_2015["year"] = 2015

        df_concat = pd.concat([df_2000, df_2005, df_2010, df_2015]).reset_index(drop=True)

        for col in ["ent_id", "population"]:
            df_concat[col] = df_concat[col].astype(int)

        df_concat["social_lag_degree"] = df_concat["social_lag_degree"].replace({
            "Muy bajo": 1,
            "Bajo": 2,
            "Medio": 3,
            "Alto": 4,
            "Muy alto": 5
        })

        return df_concat

class CONEVALSocialLagIndexStatePipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return []

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch("clickhouse-database", open("../conns.yaml"))
        dtype = {
            "ent_id":       "UInt8",
            "year":         "UInt16",
            "population":   "UInt64"
        }

        download_step = DownloadStep(
            connector="social-lag-data",
            connector_path="conns.yaml"
        )
        transform_step = TransformStep()
        load_step = LoadStep(
            "coneval_poverty", db_connector, if_exists="append", pk=["mun_id", "year"], dtype=dtype,
            nullable_list=[]
        )

        return [download_step, transform_step, load_step]