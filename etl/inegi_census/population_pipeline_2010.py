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
        df["mun_id"] = df["ent"].astype(str).str.zfill(2) + df["mun"].astype(str).str.zfill(3)
        df["mun_id_trab"] = df["ltrabpai_c"] + df["ltrabmun_c"]
        df["year"] = 2010
        df["edad"] = df["edad"].astype(int)

        # Adding work places IDs as float type column
        df["mun_id_trab"].fillna("0", inplace=True)
        df["mun_id_trab"] = df["mun_id_trab"].astype(int)
        df["edad"] = df["edad"].astype(object)

        # Turning work places IDs to 0, which are overseas
        df.loc[df["mun_id_trab"] > 33000, "mun_id_trab"] = 0
        df["mun_id_trab"].replace(pd.np.nan , 0, inplace=True)

        #Replacing empty values
        df["nivacad"].fillna("1000", inplace=True)

        # Adding columns related to work, which are not in the original DF (Exist in the 2015 intercensal census data)
        df["laboral_condition"] = ""
        df["time_to_work"] = ""
        df["transport_mean_work"] = ""
        df["time_to_ed_facilities"] = ""
        df["transport_mean_ed_facilities"] = ""

        # Transforming certains str columns into int values
        df["mun_id"] = df["mun_id"].astype(int)
        df["factor"] = df["factor"].astype(int)

        # List of columns for the next df
        params = ["sexo", "parent", "sersalud", "dhsersal1", "nivacad"]
        params_nan = ["laboral_condition", "time_to_work", "transport_mean_work", "time_to_ed_facilities", "transport_mean_ed_facilities"]
        params_int = ["mun_id", "year", "mun_id_trab"]

        # For cycle in order to change the content of a column from previous id, into the new ones (working for translate too)
        for sheet in params:
            df_l = pd.read_excel(df_labels, sheet)
            df[sheet] = df[sheet].astype(int)
            df[sheet] = df[sheet].replace(dict(zip(df_l.prev_id, df_l.id)))

        # Condense df around params list, mun_id, and sum over population (factor)
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
                                    "sexo": "sex",
                                    "nivacad": "academic_degree"}, inplace=True)

        # Setting same academic ids as year 2015
        df["academic_degree"].replace(12, 14, inplace=True)
        df["academic_degree"].replace(11, 13, inplace=True)
        df["academic_degree"].replace(10, 11, inplace=True)
        df["academic_degree"].replace(9, 10, inplace=True)

        df["time_to_work"].replace(0, pd.np.nan, inplace=True)
        df["transport_mean_work"].replace(0, pd.np.nan, inplace=True)
        df["time_to_ed_facilities"].replace(0, pd.np.nan, inplace=True)
        df["transport_mean_ed_facilities"].replace(0, pd.np.nan, inplace=True)
        df["academic_degree"].replace(1000, pd.np.nan, inplace=True)
        df["laboral_condition"].replace(0, pd.np.nan, inplace=True)
        df["mun_id_trab"].replace(0, pd.np.nan, inplace=True)
        df["age"].replace(999, pd.np.nan, inplace=True)

        # Transforming certains columns into int values
        for col in ["sex", "parent", "sersalud", "dhsersal1", "laboral_condition",
                    "time_to_work", "transport_mean_work", "mun_id_trab", "age",
                    "academic_degree", "time_to_ed_facilities", "transport_mean_ed_facilities"]:
            df[col] = df[col].astype("object")
        
        df["nationality"] = pd.np.nan
        
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
            "sex":                          "UInt8",
            "mun_id":                       "UInt16",
            "population":                   "UInt64",
            "nationality":                  "UInt8",
            "parent":                       "UInt8",
            "sersalud":                     "UInt8",
            "dhsersal1":                    "UInt8",
            "laboral_condition":            "UInt8",
            "time_to_work":                 "UInt8",
            "transport_mean_work":          "UInt8",
            "time_to_ed_facilities":        "UInt8",
            "transport_mean_ed_facilities": "UInt8",
            "mun_id_trab":                  "UInt8",
            "academic_degree":              "UInt8",
            "age":                          "UInt8",
            "year":                         "UInt16"
        }

        download_step = DownloadStep(
            connector="population-data",
            connector_path="conns.yaml"
        )
        transform_step = TransformStep()
        load_step = LoadStep(
            "inegi_population", db_connector, if_exists="append", pk=["mun_id", "sex"], dtype=dtype, 
            nullable_list=["age", "time_to_work", "transport_mean_work", "laboral_condition", "mun_id_trab", "academic_degree",
            "time_to_ed_facilities","transport_mean_ed_facilities", "nationality"]
        )

        return [download_step, transform_step, load_step]
