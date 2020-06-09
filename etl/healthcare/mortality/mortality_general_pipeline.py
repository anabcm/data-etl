
import pandas as pd
from simpledbf import Dbf5
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import DownloadStep
from bamboo_lib.steps import LoadStep

COLS = {"ent_ocurr": "ent_id", 
        "mun_ocurr": "mun_id", 
        "sexo": "sex", 
        "edad": "age", 
        "ocupacion": "occupation", 
        "escolarida": "scholarly"}


class TransformStep(PipelineStep):
    def run_step(self, prev, params):

        dbf = Dbf5(prev, codec="latin-1")
        df = dbf.to_dataframe()
        df.columns = df.columns.str.lower()
        df = df[list(COLS.keys())].copy()
        df.rename(columns=COLS, inplace=True)
        df["age"] = df["age"].astype(int)

        pre_age_conversion = {
            1097: 0, # minutos
            1098: 999, # horas no especificadas
            2098: 999, # dias no especificados
            3098: 999, # meses no especificados
            4998: 999 # añios no especificados
        }

        def age_conversion(code):
            if code >= 4001:
                # 1+ años de vida
                return int(str(code)[1:])
            elif (code <= 3011) & (code > 999):
                # entre 0 horas y 11 meses de vida
                return 0
            else:
                return code

        df["age"] = df["age"].replace(pre_age_conversion)

        df["age"] = df["age"].apply(lambda x: age_conversion(x))

        df["ent_id"] = df["ent_id"].astype(str).str.zfill(2)
        df["mun_id"] = df["mun_id"].astype(str).str.zfill(3)
        df["mun_id"] = df["ent_id"] + df["mun_id"]
        df["mun_id"] = df["mun_id"].astype(int)
        df.drop(columns=["ent_id"], inplace=True)

        df["general_deaths"] = 1
        df["general_deaths_over_one"] = 1
        df["one_year_deaths"] = 0
        df.loc[df["age"] == 0, "one_year_deaths"] = 1
        df.loc[df["one_year_deaths"] == 1, "general_deaths_over_one"] = 0

        df["year"] = params.get("year")

        return df

class MortalityGeneralPipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return [
          Parameter(label="year", name="year", dtype=str),
          Parameter(label="partial-year", name="partial_year", dtype=str),
        ]

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch("clickhouse-database", open("../../conns.yaml"))

        dtype = {
            "mun_id":                   "UInt16",
            "sex":                      "UInt8",
            "year":                     "UInt16",
            "general_deaths":           "UInt16",
            "one_year_deaths":          "UInt16",
            "general_deaths_over_one":  "UInt16"
        }

        download_step = DownloadStep(
            connector="mortality-data",
            connector_path="conns.yaml"
        )
        transform_step = TransformStep()
        load_step = LoadStep(
            "inegi_general_mortality", db_connector, if_exists="drop", pk=["mun_id", "sex", "year"], dtype=dtype
        )

        return [download_step, transform_step, load_step]

if __name__ == "__main__":
    pp = MortalityGeneralPipeline()
    for year in range(1990, 2018 + 1):
        pp.run({"year": year,
                "partial_year": str(year)[2:]})