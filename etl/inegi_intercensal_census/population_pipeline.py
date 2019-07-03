import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import DownloadStep
from bamboo_lib.steps import LoadStep


class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        df_labels = pd.ExcelFile("https://docs.google.com/spreadsheets/d/e/2PACX-1vR08Js9Sh4nNTMe5uBcsDUFedG5MOjIf90p6EHAr1_sWY5kpnI3xUvyPHzQpTEUrXz1pskaoc0uyea6/pub?output=xlsx")

        df = pd.read_csv(prev, dtype=str, index_col=None, header=0, encoding="latin-1")
        df.columns = df.columns.str.lower()

        # Adding IDs columns and renaming factor as population
        df["loc_id"] = df["ent"] + df["mun"] + df["loc50k"]

        # Transforming certains str columns into int values
        df["loc_id"] = df["loc_id"].astype(int)
        df["factor"] = df["factor"].astype(int)

        # Replacing NaN values with "0" in order to pass GroupBy method
        df.fillna("0", inplace=True)

        # List of columns for the next df
        params            = ["sexo", "parent", "sersalud", "dhsersal1", "nacionalidad", "conact", "tie_traslado_trab", "med_traslado_trab1"]
        params_translated = ["sex", "parent", "sersalud", "dhsersal1", "nationality", "laboral_condition", "time_to_work", "transport_mean_work"]

        # For cycle in order to change the content of a column from previous id, into the new ones (working for translate too)
        for sheet in params:
            df_l = pd.read_excel(df_labels, sheet)
            df[sheet] = df[sheet].astype(int)
            df[sheet] = df[sheet].replace(dict(zip(df_l.prev_id, df_l.id)))

        # Renaming of certains columns
        df.rename(index=str, columns={"factor": "population", "nacionalidad": "nationality", "sexo": "sex", 
                                        "conact": "laboral_condition", "tie_traslado_trab": "time_to_work",
                                        "med_traslado_trab1": "transport_mean_work"}, inplace=True)

        # Condense df around params list, mun_id and loc_id, and sum over population (factor)
        df = df.groupby(params_translated + ["loc_id"]).sum().reset_index(col_fill="ffill")

        # Turning back NaN values
        df.replace(0, pd.np.nan, inplace=True)

        # Transforming certains columns into int values
        for col in ["sex", "parent", "sersalud", "dhsersal1", "nationality", "laboral_condition", "time_to_work", "transport_mean_work"]:
            df[col] = df[col].astype('object')

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
            "nationality":         "UInt8",
            "laboral_condition":   "UInt8",
            "time_to_work":        "UInt8",
            "transport_mean_work": "UInt8",
        }

        download_step = DownloadStep(
            connector="population-data",
            connector_path="conns.yaml"
        )
        transform_step = TransformStep()
        load_step = LoadStep(
            "inegi_population", db_connector, if_exists="append", pk=["loc_id", "sex"], dtype=dtype, 
            nullable_list=["parent", "laboral_condition", 
                          "time_to_work", "transport_mean_work", 
                          "sersalud", "dhsersal1", "nationality"]
        )

        return [download_step, transform_step, load_step]
