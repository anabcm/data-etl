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
        df.rename(columns={'factor': 'population'}, inplace=True)

        primary_cols = ["ent", "mun", "population"] + df_labels.sheet_names

        final_df = df[primary_cols]
        final_df["mun_id"] = final_df["ent"].astype(str) + final_df["mun"].astype(str).str.zfill(3)
        final_df["mun_id"] = final_df["mun_id"].astype(int)
        final_df = final_df.drop(columns=["ent", "mun"])

        # Add new label columns to final_df
        for label in df_labels.sheet_names:
            d = pd.read_excel(df_labels, label)
            final_df[label] = final_df[label].replace(dict(zip(d.prev_id, d.id)))

        # Group rows to get final population sum
        grouped = list(final_df)
        grouped.remove("population")
        final_df = final_df.groupby(grouped).sum().reset_index()

        return final_df


class PopulationPipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return [
            Parameter(label="Index", name="index", dtype=str),
        ]

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch('clickhouse-database', open("etl/conns.yaml"))

        download_step = DownloadStep(
            connector='population-data',
            connector_path='etl/inegi_intercensal_census/conns.yaml'
        )
        transform_step = TransformStep()

        return [download_step, transform_step]
