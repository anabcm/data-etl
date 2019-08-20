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
        df = pd.read_excel(excel, "Municipios", header=2)

        df = df[~df["Municipio"].isna()].copy()
        df = df[df["Municipio"] != "Clave"]

        headers = [
            "mun_id", "population", "population_illiterate", "population_6_14_school", "population_15_incomplete_school", 
            "no_health_services", "dirt_floor", "no_toilet", "no_water_supply_network", "no_sewer_system", 
            "no_electrical_energy", "no_washing_machine", "no_fridge", "social_lag_index", "social_lag_degree"
        ]

        df_2000 = df[["Municipio", "Población total", "Indicadores de rezago social (porcentaje)"] + ["Unnamed: {}".format(13 + 4*i) for i in range(10)] + ["Índice de rezago social", "Grado de rezago social"]].copy()
        df_2000.columns = headers
        df_2000["year"] = 2000

        df_2005 = df[["Municipio"] + ["Unnamed: {}".format(5 + 4*i) for i in range(14)]].copy()
        df_2005.columns = headers
        df_2005["year"] = 2005

        df_2010 = df[["Municipio"] + ["Unnamed: {}".format(6 + 4*i) for i in range(14)]].copy()
        df_2010.columns = headers
        df_2010["year"] = 2010

        df_2015 = df[["Municipio"] + ["Unnamed: {}".format(7 + 4*i) for i in range(14)]].copy()
        df_2015.columns = headers
        df_2015["year"] = 2015

        df_concat = pd.concat([df_2000, df_2005, df_2010, df_2015]).reset_index(drop=True)

        df_concat["population"] = df_concat["population"].replace("ND", pd.np.nan).astype(object)
        df_concat["mun_id"] = df_concat["mun_id"].astype(int)

        df_concat["social_lag_degree"] = df_concat["social_lag_degree"].replace({
            "Muy bajo": 1,
            "Bajo": 2,
            "Medio": 3,
            "Alto": 4,
            "Muy alto": 5
        })

        for col in ["population_illiterate", "population_6_14_school", "population_15_incomplete_school", 
            "no_health_services", "dirt_floor", "no_toilet", "no_water_supply_network", "no_sewer_system", 
            "no_electrical_energy", "no_washing_machine", "no_fridge", "social_lag_index"]:
            df_concat[col] = df_concat[col].replace({"ND": pd.np.nan, "ND ": pd.np.nan}).astype(float)

        df_concat["social_lag_degree"] = df_concat["social_lag_degree"].replace("ND", pd.np.nan).astype(object)

        return df_concat

class CONEVALSocialLagIndexMunPipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return []

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch("clickhouse-database", open("../conns.yaml"))
        dtype = {
            "mun_id":               "UInt16",
            "year":                 "UInt16",
            "population":           "UInt64",
            "social_lag_degree":    "UInt8"
        }

        download_step = DownloadStep(
            connector="social-lag-data",
            connector_path="conns.yaml"
        )
        transform_step = TransformStep()
        load_step = LoadStep(
            "coneval_social_lag_mun", db_connector, if_exists="drop", pk=["mun_id", "year"], dtype=dtype,
            nullable_list=["population", "social_lag_degree"]
        )

        return [download_step, transform_step, load_step]