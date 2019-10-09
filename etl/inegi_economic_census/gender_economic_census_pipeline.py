import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import DownloadStep
from bamboo_lib.steps import LoadStep


class TransformStep(PipelineStep):
    def run_step(self, prev, params):

        df = pd.read_csv(prev, index_col=None, encoding="latin-1", low_memory=False)

        ren_cols = {
          "H001B": "1", "H000B": "2", "H010B": "3", "H101B": "4", "H203B": "5", "H020B": "6", "I000B": "7", "I100B": "8", "I200B": "9",
          "H001C": "1", "H000C": "2", "H010C": "3", "H101C": "4", "H203C": "5", "H020C": "6", "I000C": "7", "I100C": "8", "I200C": "9"}

        # Deleting no usable rows
        df.drop_duplicates(subset=["Año Censal", "Entidad", "Municipio", "Actividad Económica", "UE Unidades económicas"], inplace=True)
        df.drop(["Unnamed: 118"], axis=1, inplace=True)
        df.drop(df[df["Entidad"]=="00 Total Nacional"].index, inplace=True)

        # Renaming columns
        temp = list(df.columns[:4])
        for i in range(4, len(df.columns)):
            temp.append(df.columns[i].split(" ")[0])
        df.columns = temp

        # Deleting duplicated columns, and empty rows
        df = df.loc[:, ~df.columns.duplicated()]
        df.drop(df.columns[[-1,-2,-3]], axis=1, inplace=True)

        # Drops "Total" rows and empty columns
        df.drop(df.loc[df["Actividad Económica"].str.match('Total nacional') | df["Actividad Económica"].str.match('Total municipal')].index, inplace=True)
        df=df[pd.notnull(df["Año Censal"])]
        df["Año Censal"]=df["Año Censal"].astype(int)

        # Creating mun_id code
        piv_ent = df["Entidad"].str.split(" ", n=1, expand=True)
        piv_mun = df["Municipio"].str.split(" ", n=1, expand=True)
        df["Entidad"] = piv_ent[0] + piv_mun[0]
        df.drop(["Municipio"], axis=1, inplace=True)

        # Renaming columns
        df.rename(index=str, columns={"Año Censal": "year", "Entidad": "mun_id", "Actividad Económica": "national_industry_id"}, inplace=True)

        # Getting national industry id
        piv_class = df["national_industry_id"].str.split(" ", n=1, expand=True)
        df["national_industry_id"] = piv_class[0]

        # Useless 3 columns
        df.drop(df.columns[[-1,-2,-3]], axis=1, inplace=True)

        # Gender lists
        males_  = ["H001B", "H000B", "H010B", "H101B", "H203B", "H020B", "I000B", "I100B", "I200B"]
        females_= ["H001C", "H000C", "H010C", "H101C", "H203C", "H020C", "I000C", "I100C", "I200C"]

        # Creating gender based dataframes
        df_males = df[["year", "mun_id", "national_industry_id"] + males_].copy()
        df_females = df[["year", "mun_id", "national_industry_id"] + females_].copy()

        # Setting sex id's
        df_males["sex"] = 1
        df_females["sex"] = 2

        # Melt of the male and female dataframes, pre concat
        df_males  =  pd.melt(df_males, id_vars = ["year", "mun_id", "national_industry_id", "sex"],
                          value_vars = ["H001B", "H000B", "H010B", "H101B", "H203B", "H020B", "I000B", "I100B", "I200B"])

        df_females = pd.melt(df_females, id_vars = ["year", "mun_id", "national_industry_id", "sex"],
                            value_vars = ["H001C", "H000C", "H010C", "H101C", "H203C", "H020C", "I000C", "I100C", "I200C"])

        # Concat step to unite male and female dataframes
        df = pd.concat([df_males, df_females], axis=0, join='outer', ignore_index=False, keys=None,
                  levels=None, names=None, verify_integrity=False, copy=True)

        # Renaming columns from the melt step
        df.rename(index=str, columns={"variable": "classification", "value": "count"}, inplace=True)

        # Replacing codes from classification to the actual meaning
        df["classification"].replace(dict(ren_cols), inplace=True)

        # Groupby step
        df = df.groupby(["year", "mun_id", "national_industry_id", "sex", "classification"]).sum().reset_index(col_fill="ffill")

        # Setting types
        for item in ["year", "mun_id", "sex", "count", "classification"]:
            df[item] = df[item].astype(int)

        return df

class EconomicCensusGenderPipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return []

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch("clickhouse-database", open("../conns.yaml"))

        dtype = {
            "mun_id":                               "UInt16",
            "year":                                 "UInt16",
            "sex":                                  "UInt8",
            "national_industry_id":                 "String",
            "classification":                       "UInt8",
            "count":                                "UInt32"
        }

        download_step = DownloadStep(
            connector="dataset",
            connector_path="conns.yaml"
        )
        
        transform_step = TransformStep()
        load_step = LoadStep(
            "inegi_economic_census_gender", db_connector, if_exists="append", pk=["mun_id", "year"], dtype=dtype, 
        )

        return [download_step, transform_step, load_step]