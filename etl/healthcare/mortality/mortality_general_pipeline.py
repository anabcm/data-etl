import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import DownloadStep
from bamboo_lib.steps import LoadStep


class TransformStep(PipelineStep):
    def run_step(self, prev, params):

        # Renaming columns from first quarter to match the rest of the year
        df = pd.read_excel(prev, index_col=None, header=0)

        # Columns with columns besides annual totals
        _years = [str(i) for i in range(1994, 2017)]

        # Droping rows related to "Estados Unidos Mexicanos" or national/entity/municipality totals
        df.drop(df.loc[(df["entidad"] == 0) | (df["municipio"] == 0)].index, inplace=True)

        # Creating news geo ids
        df["mun_id"] = df["entidad"].astype("str").str.zfill(2) + df["municipio"].astype("str").str.zfill(3)

        # Droping used columns, as well years that only had national totals
        df.drop(["entidad", "municipio", "desc_entidad", "desc_municipio", "indicador", "unidad_medida",
                "1990", "1991", "1992", "1993"], axis=1, inplace=True)

        # Keeping columns related to general deaths by gender
        df_1 = df[(df["id_indicador"] == 1002000031) | (df["id_indicador"] == 1002000032) | (df["id_indicador"] == 1002000033)]

        # Division per gender (totals) [1: male, 2: female, 0: unknown]
        df_1["id_indicador"].replace({1002000031: 1, 1002000032: 2, 1002000033: 0}, inplace=True)

        # Melt step in order to get file in tidy data format
        df_1 = pd.melt(df_1, id_vars = ["mun_id", "id_indicador"], value_vars = _years)

        # Renaming columns from spanish to english
        df_1.rename(columns = {"id_indicador": "sex_id", "variable": "year", "value": "general_deaths"}, inplace=True)

        # Groupby step
        df_1 = df_1.groupby(["mun_id", "sex_id", "year"]).sum().reset_index(col_fill="ffill")

        # Division per gender (1 year olds)
        df_2 = df[(df["id_indicador"] == 1002000035) | (df["id_indicador"] == 1002000036)| (df["id_indicador"] == 1002000037)]

        # Division per gender (totals) [1: male, 2: female, 0: unknown]
        df_2["id_indicador"].replace({1002000035: 1, 1002000036: 2, 1002000037: 0}, inplace=True)

        # Melt step in order to get file in tidy data format
        df_2 = pd.melt(df_2, id_vars = ["mun_id", "id_indicador"], value_vars = _years)

        # Renaming columns from spanish to english
        df_2.rename(columns = {"id_indicador": "sex_id", "variable": "year", "value": "one_year_deaths"}, inplace=True)

        # Groupby step
        df_2 = df_2.groupby(["mun_id", "year", "sex_id"]).sum().reset_index(col_fill="ffill")

        # Create code to merge step
        df_1["code"] = df_1["mun_id"].astype("str").str.zfill(5) + df_1["year"].astype("str") + df_1["sex_id"].astype("str")
        df_2["code"] = df_2["mun_id"].astype("str").str.zfill(5) + df_2["year"].astype("str") + df_2["sex_id"].astype("str")

        # Merge step
        df = pd.merge(df_1, df_2[["code", "one_year_deaths"]], on="code", how="left")

        # Droping code column
        df.drop(["code"], axis=1, inplace=True)

        # Turning to int values
        for item in ["mun_id", "sex_id", "year", "general_deaths", "one_year_deaths"]:
            df[item] = df[item].astype(int)

        return df

class MortalityGeneralPipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return []

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch("clickhouse-database", open("../../conns.yaml"))

        dtype = {
            "mun_id":                   "UInt16",
            "sex_id":                   "UInt8",
            "year":                     "UInt16",
            "general_deaths":           "UInt16",
            "one_year_deaths":          "UInt16"
        }

        download_step = DownloadStep(
            connector="mortality-data",
            connector_path="conns.yaml"
        )
        transform_step = TransformStep()
        load_step = LoadStep(
            "inegi_general_mortality", db_connector, if_exists="drop", pk=["mun_id", "sex_id", "year"], dtype=dtype
        )

        return [download_step, transform_step, load_step]