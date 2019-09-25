import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import DownloadStep
from bamboo_lib.steps import LoadStep


class TransformStep(PipelineStep):
    def run_step(self, prev, params):

        # List to useful columns from income and household
        income_cols = ["folioviv", "foliohog", "ing_1", "ing_2", "ing_3", "ing_4", "ing_5", "ing_6"]
        household_cols = ["folioviv", "foliohog", "ubica_geo", "factor"]

        # Loading income file 
        df_1 = pd.read_csv(prev[0], index_col=None, header=0, encoding="latin-1", usecols = income_cols)

        # Filling empty cells about not answered income
        for item in ["ing_1", "ing_2", "ing_3", "ing_4", "ing_5", "ing_6"]:
            df_1[item].replace(" ", 0 ,inplace=True)
            df_1[item] = df_1[item].astype(int)

        # Calculating average income per month
        df_1["monthly_average"] = (df_1["ing_1"] + df_1["ing_2"] + df_1["ing_3"] + df_1["ing_4"] + df_1["ing_5"] + df_1["ing_6"])/6

        # Droping used columns
        df_1.drop(["ing_1", "ing_2", "ing_3", "ing_4", "ing_5", "ing_6"], axis=1, inplace=True)

        # Groupby step
        df_1 = df_1.groupby(["folioviv", "foliohog"]).sum().reset_index()

        # Loading enigh household dataframe in order to get mun_id and factor columns
        df_2 = pd.read_csv(prev[1], index_col=None, header=0, encoding="latin-1", usecols = household_cols)

        # Turning Factor to int value
        df_2["factor"] = df_2["factor"].astype(int)

        # Creating unique code column for the merge method
        df_1["code"] = df_1["folioviv"].astype("str") + df_1["foliohog"].astype("str").str.zfill(3)
        df_2["code"] = df_2["folioviv"].astype("str") + df_2["foliohog"].astype("str").str.zfill(3)

        # Merge step
        df = pd.merge(df_2, df_1[["code", "monthly_average"]], on="code", how="left")

        # Droping already used columns
        df.drop(["folioviv", "foliohog", "code"], axis=1, inplace=True)

        # Renaming columns to the used standard
        df.rename(index=str, columns={"ubica_geo": "mun_id", "factor": "population"}, inplace=True)

        # Getting actual mun_id deleting last 4 digits from the values
        df["mun_id"] = df["mun_id"].astype(str).str.zfill(9)
        df["mun_id"] = df["mun_id"].str.slice(0,5)

        # Adding respective year to the Dataframes, given Inegis update (2016-2018)
        df["year"] = params["year"]

        # Setting all columns to int 
        for item in ["mun_id", "population", "monthly_average", "year"]:
            df[item] = df[item].astype(int)

        return df

class EnighIncomeHousePipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return [
          Parameter(label="Year", name="year", dtype=str)
        ]

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch("clickhouse-database", open("../conns.yaml"))

        dtype = {
            "mun_id":                   "UInt16",
            "monthly_average":          "UInt32",
            "population":               "UInt16",
            "year":                     "UInt16"
        }

        download_step = DownloadStep(
            connector=["enigh-income", "enigh-household"],
            connector_path="conns.yaml"
        )
        transform_step = TransformStep()
        load_step = LoadStep(
            "inegi_enigh_household_income", db_connector, if_exists="drop", pk=["mun_id"], dtype=dtype
        )

        return [download_step, transform_step, load_step]