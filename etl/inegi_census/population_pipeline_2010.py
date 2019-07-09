import pandas as pd
from simpledbf import Dbf5 #required given data files
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import DownloadStep
from bamboo_lib.steps import LoadStep


class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        df_labels = pd.ExcelFile("https://docs.google.com/spreadsheets/d/e/2PACX-1vSYxBmHW5xXkzhVL3X5N21EWWVJKzOkaCfEERaG5lWpmgdx6-Sjcxf7FA7uV1j-_EJIeWZmGbMMDeJh/pub?output=xlsx")

        dbf = Dbf5(prev, codec="latin-1")
        df = dbf.to_dataframe()
        df.columns = df.columns.str.lower()

        # Adding ID columns, year and age(edad)
        df["loc_id"] = df["ent"] + df["mun"] + df["loc50k"]
        df["year"] = 2010
        df["edad"] = df["edad"].astype(int)

        # Adding work places IDs as float type column
        df["mun_id_trab"] = df["ltrabpai_c"] + df["ltrabmun_c"]
        df["mun_id_trab"] = df["mun_id_trab"].astype(float)

        # Turning work places IDs to 0, which are overseas
        df.loc[df["mun_id_trab"] > 33000, "mun_id_trab"] = 0
        df["mun_id_trab"].replace(pd.np.nan , 0, inplace=True)

        # Adding columns related to work, which are not in the original DF (Exist in the 2015 intercensal census data)
        df["laboral_condition"] = ""
        df["time_to_work"] = ""
        df["transport_mean_work"] = ""

        # Transforming certains str columns into int values
        df["loc_id"] = df["loc_id"].astype(int)
        df["factor"] = df["factor"].astype(int)

        # List of columns for the next df
        params = ["sexo", "parent", "sersalud", "dhsersal1"]
        params_nan = ["laboral_condition", "time_to_work", "transport_mean_work"]
        params_int = ["loc_id", "year", "mun_id_trab"]

        # For cycle in order to change the content of a column from previous id, into the new ones (working for translate too)
        for sheet in params:
            df_l = pd.read_excel(df_labels, sheet)
            df[sheet] = df[sheet].astype(int)
            df[sheet] = df[sheet].replace(dict(zip(df_l.prev_id, df_l.id)))


        # Condense df around params list, mun_id and loc_id, and sum over population (factor)
        df = df.groupby(params + params_nan + params_int + ["edad"]).sum().reset_index(col_fill="ffill")

        # Filling empty values with NaN, an replacing 0 values in mun_id_trab
        df["laboral_condition"] = pd.np.nan
        df["time_to_work"] = pd.np.nan
        df["transport_mean_work"] = pd.np.nan
        df["mun_id_trab"].replace(0, pd.np.nan, inplace=True)


        # Renaming of certains columns
        df.rename(index=str, columns={
                                    "factor": "population",
                                    "edad": "age",
                                    "sexo": "sex"}, inplace=True)

        # Not answered age values, turned to text (Column as object type)
        df["age"].replace(999, "Edad no especificada", inplace=True)

        # Transforming certains columns into int values
        for col in ["sex", "parent", "sersalud", "dhsersal1", "laboral_condition", "time_to_work", "transport_mean_work", "mun_id_trab", "age"]:
            df[col] = df[col].astype("object")

        return df

class PopulationPipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return [
            Parameter(label="Index", name="index", dtype=str)
        ]

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch("clickhouse-database", open("../conns.yaml"))

        dtype = {
            "sex":                 "UInt8",
            "loc_id":              "UInt32",
            "population":          "UInt64",
            "parent":              "UInt32",
            "sersalud":            "UInt32",
            "dhsersal1":           "UInt32",
            "laboral_condition":   "UInt8",
            "time_to_work":        "UInt8",
            "transport_mean_work": "UInt8",
            "mun_id_trab":         "UInt8",
            "age":                 "UInt8",
            "year":                "UInt8",
        }

        download_step = DownloadStep(
            connector="population-data",
            connector_path="conns.yaml"
        )
        transform_step = TransformStep()
        load_step = LoadStep(
            "inegi_population", db_connector, if_exists="append", pk=["loc_id", "sex"], dtype=dtype, 
            nullable_list=["time_to_work", "transport_mean_work", "laboral_condition", "mun_id_trab"]
        )

        return [download_step, transform_step, load_step]
