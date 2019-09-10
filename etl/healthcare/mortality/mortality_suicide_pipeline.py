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
        _years = [str(i) for i in range(1994,2017)]

        # Droping rows related to "Estados Unidos Mexicanos" or national/entity totals
        df.drop(df.loc[(df["entidad"] == 0)].index, inplace=True)

        # Creating news geo ids
        df["ent_id"] = df["entidad"].astype("str").str.zfill(2)

        # Droping used columns, as well years that only had national totals
        df.drop(["entidad", "municipio", "desc_entidad", "desc_municipio", "indicador", "unidad_medida",
                "1990", "1991", "1992", "1993"], axis=1, inplace=True)

        # Melt step in order to get file in tidy data format
        df = pd.melt(df, id_vars = ["ent_id", "id_indicador"], value_vars = _years)

        # Creating 2 dataframes in order to reorder based on ent_id and year
        df_1 = df.loc[df["id_indicador"] == 6200240338]
        df_2 = df.loc[df["id_indicador"] == 6200240526]
        df_3 = df.loc[df["id_indicador"] == 6200240468]

        # Create and "code" value to use melt method
        df_1["code"] = df_1["ent_id"] + df_1["variable"].astype("str")
        df_2["code"] = df_2["ent_id"] + df_2["variable"].astype("str")
        df_3["code"] = df_3["ent_id"] + df_3["variable"].astype("str")

        # Renaming columns from spanish to english
        df_1.rename(columns = {"variable": "year", "value": "suicides"}, inplace=True)
        df_2.rename(columns = {"variable": "year", "value": "rate_violent_deaths"}, inplace=True)
        df_3.rename(columns = {"variable": "year", "value": "male_overmortality_index"}, inplace=True)

        # Merge method to get one dataframe
        df = pd.merge(df_1, df_2[["code", "rate_violent_deaths"]], on="code", how="left")
        df = pd.merge(df, df_3[["code", "male_overmortality_index"]], on="code", how="left")

        # Droping last columns
        df.drop(["id_indicador", "code"], axis=1, inplace=True)

        # Setting types
        for item in ["ent_id", "suicides", "year"]:
            df[item] = df[item].astype(int)

        df["rate_violent_deaths"] = df["rate_violent_deaths"].astype(float)
        df["male_overmortality_index"] = df["male_overmortality_index"].astype(float)

        return df

class MortalitySuicidePipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return []

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch("clickhouse-database", open("../../conns.yaml"))

        dtype = {
            "ent_id":                       "UInt8",
            "year":                         "UInt16",
            "suicides":                     "UInt8",
            "rate_violent_deaths":          "Float32",
            "male_overmortality_index":     "Float32"
        }

        download_step = DownloadStep(
            connector="mortality-data",
            connector_path="conns.yaml"
        )
        transform_step = TransformStep()
        load_step = LoadStep(
            "inegi_suicides_over_mortality", db_connector, if_exists="drop", pk=["ent_id", "year"], dtype=dtype
        )

        return [download_step, transform_step, load_step]