import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import DownloadStep
from bamboo_lib.steps import LoadStep

sex_dict = {
     "male_population": 1,
     "female_population": 2
}

class TransformStep(PipelineStep):
    def run_step(self, prev, params):

        df = pd.read_csv(prev)

        # Standarize column names
        df.columns = [x.lower() for x in df.columns]

        # Get "entidad", "mun", "loc", "nom_mun", "nom_loc", "pobfem", "pobmas"
        df = df[["entidad", "mun", "loc", "nom_mun", "nom_loc", "pobfem", "pobmas"]]

        # Filter of non use rows (totals)
        #df = df.loc[(~df['nom_mun'].str.contains('Total')) & (~df['nom_loc'].str.contains('Total')) & (~df['nom_loc'].str.contains('Localidades de '))].copy()
        df = df.loc[df['nom_loc'].str.contains('Total del Municipio')].copy()

        # Changing population dtype
        df.pobfem = df.pobfem.astype(int)
        df.pobmas = df.pobmas.astype(int)

        # Adding zero's to IDs columns
        df["mun"] = df["mun"].astype(str).str.zfill(3)

        # Slicing columns and creating location ID
        df["mun_id"] = df["entidad"].astype(str) + df["mun"]

        df.rename(index=str, columns={"pobfem": "female_population", "pobmas": "male_population"}, inplace=True)

        df = df[["mun_id", "female_population", "male_population"]].copy()

        # Transforming str columns into int values
        df["mun_id"] = df["mun_id"].astype(int)
        df["year"] = params.get("year")

        # Creating sex column
        df = df.melt(id_vars=["mun_id", "year"], var_name="sex", value_name="population")
        df.sex = df.sex.replace(sex_dict)

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
            "year":           "UInt16",
            "sex":            "UInt8",
            "population":     "UInt64",
        }

        download_step = DownloadStep(
            connector="population-data",
            connector_path="conns.yaml"
        )
        transform_step = TransformStep()
        load_step = LoadStep(
            "inegi_population_total", db_connector, if_exists="append", pk=["mun_id"], dtype=dtype
        )

        return [download_step, transform_step, load_step]