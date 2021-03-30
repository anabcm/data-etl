import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import DownloadStep
from bamboo_lib.steps import LoadStep


class TransformStep(PipelineStep):
    def run_step(self, prev, params):

        df = pd.read_csv(prev)

        # Standarize column names
        df.columns = [x.lower() for x in df.columns]

        # Get "entidad", "mun", "loc", "nom_mun", "nom_loc", "pobtot"
        df = df[["entidad", "mun", "loc", "nom_mun", "nom_loc", "pobtot"]]

        # Filter of non use rows (totals)
        df = df.loc[(~df['nom_mun'].str.contains('Total')) & (~df['nom_loc'].str.contains('Total')) & (~df['nom_loc'].str.contains('Localidades de '))].copy()

        # Adding zero's to IDs columns
        df["mun"] = df["mun"].astype(str).str.zfill(3)

        # Slicing columns and creating location ID
        df["mun_id"] = df["entidad"].astype(str) + df["mun"]

        df.rename(index=str, columns={"pobtot": "population"}, inplace=True)

        df = df[["mun_id", "population"]].copy()

        # Transforming certains str columns into int values
        df["mun_id"] = df["mun_id"].astype(int)
        df["population"] = df["population"].astype(int)
        df["year"] = params.get("year")

        return df

class Population2010Pipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return [
            Parameter(label="Index", name="index", dtype=str),
            Parameter(label="Year", name="year", dtype=str)
        ]

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch("clickhouse-database", open("../conns.yaml"))

        dtype = {
            "mun_id":         "UInt16",
            "population":     "UInt64",
            "year":           "UInt16"
        }

        download_step = DownloadStep(
            connector="population-data",
            connector_path="conns.yaml"
        )
        transform_step = TransformStep()
        load_step = LoadStep(
            "inegi_population_total_2010", db_connector, if_exists="append", pk=["mun_id"], dtype=dtype
        )

        return [download_step, transform_step, load_step]