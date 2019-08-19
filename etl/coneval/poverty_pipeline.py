import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import DownloadStep
from bamboo_lib.steps import LoadStep


class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        df = pd.read_csv(prev, encoding="latin-1")

        columns={
            "clave_municipio": "mun_id",
            "poblacion": "population",
            "pobreza_pob": "poverty",
            "pobreza_e_pob": "extreme_poverty",
            "pobreza_m_pob": "moderate_poverty",
            "vul_car_pob": "vulnerable_deficiencies",
            "vul_ing_pob": "vulnerable_income",
            "npnv_pob": "no_poverty_vulnerable",
            "ic_rezedu_pob": "educational_backwardness",
            "ic_cv_pob": "deprivation_quality_housing_spaces",
            "ic_asalud_pob": "deprivation_health_services",
            "ic_segsoc_pob": "deprivation_social_security",
            "ic_sbv_pob": "deprivation_basic_services_housing",
            "ic_ali_pob": "deprivation_food_access",
            "carencias_pob": "at_least_one_deficiency",
            "carencias3_pob": "at_least_three_deficiencies",
            "plb_pob": "income_below_welfare_line",
            "plbm_pob": "income_below_min_welfare_line"
        }

        df = df.rename(columns=columns)
        df = df[list(columns.values())]
        df["year"] = params["year"]

        cols = list(columns.values()).copy()
        cols.remove("mun_id")

        for col in cols:
            df[col] = df[col].str.replace(",", "").astype(int)
        df["year"] = df["year"].astype(int)

        return df

class CONEVALPovertyPipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return [
            Parameter(label="Year", name="year", dtype=str)
        ]

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch("clickhouse-database", open("../conns.yaml"))
        dtype = {
            "mun_id":   "UInt16",
            "year":     "UInt16"
        }

        download_step = DownloadStep(
            connector="poverty-data",
            connector_path="conns.yaml"
        )
        transform_step = TransformStep()
        load_step = LoadStep(
            "coneval_poverty", db_connector, if_exists="append", pk=["mun_id", "year"], dtype=dtype
        )

        return [download_step, transform_step, load_step]