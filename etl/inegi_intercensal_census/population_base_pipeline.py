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

        df = pd.read_csv(prev, index_col=None, header=0, encoding='latin-1')
        df.columns = df.columns.str.lower()

        # Adding IDs columns and renaming factor as population
        df["mun_id"] = df["ent"] + df["mun"]
        df["loc_id"] = df["mun_id"] + df["loc50k"]
        df.rename(index=str, columns={"factor":"population"}, inplace=True)

        # Transforming certains str columns into int values
        df["mun_id"] = df["mun_id"].astype(int)
        df["loc_id"] = df["loc_id"].astype(int)
        df["population"] = df["population"].astype(int)

        # List of columns for the next df
        params = ["sexo", "parent", "sersalud", "dhsersal1", "nacionalidad"]

        # For cycle in order to change the content of a column from previous id, into the new ones (working for translate too)
        for sheet in params:
          df_l = pd.read_excel(df_labels, sheet)
          df[sheet] = df[sheet].astype(int)
          df[sheet] = df[sheet].replace(dict(zip(df_l.prev_id, df_l.id)))

        # Condense df around params list, mun_id and loc_id, and sum over population (factor)
        df = df.groupby(params + ["mun_id", "loc_id"]).sum().reset_index()

        return df


class PopulationPipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return [
            Parameter(label="Index", name="index", dtype=str),
        ]

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch('clickhouse-database', open("etl/conns.yaml"))

        dtype = {
            "sexo":          "UInt8",
            "mun_id":        "UInt8",
            "loc_id":        "UInt32",
            "population":    "UInt32",
            "parent":        "UInt8",
            "sersalud":      "UInt8",
            "dhsersal1":     "UInt8",
            "nacionalidad":  "UInt8"
        }

        download_step = DownloadStep(
            connector='population-data',
            connector_path='etl/inegi_intercensal_census/conns.yaml'
        )
        transform_step = TransformStep()
        load_step = LoadStep(
            "inegi_intercensal_population", db_connector, if_exists="append", pk=['sex', 'mun_id'], dtype=dtype
        )

        return [download_step, transform_step, load_step]
