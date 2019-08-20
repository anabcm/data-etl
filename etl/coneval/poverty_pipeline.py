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
            "vul_car_pob": "vulnerable_lacks",
            "vul_ing_pob": "vulnerable_income",
            "npnv_pob": "no_vulnerable",
            "ic_rezedu_pob": "educational_backwardness",
            "ic_cv_pob": "deprivation_quality_housing_spaces",
            "ic_asalud_pob": "deprivation_health_services",
            "ic_segsoc_pob": "deprivation_social_security",
            "ic_sbv_pob": "deprivation_basic_services_housing",
            "ic_ali_pob": "deprivation_food_access",
            "carencias_pob": "at_least_one_lack",
            "carencias3_pob": "at_least_three_lacks",
            "plb_pob": "income_below_welfare_line",
            "plbm_pob": "income_below_min_welfare_line"
        }

        df = df.rename(columns=columns)
        df = df[list(columns.values())]
        df["year"] = params["year"]

        cols = list(columns.values()).copy()
        cols.remove("mun_id")

        for col in cols:
            df[col] = df[col].str.replace(",", "")
            df[col] = df[col].replace("n.d", pd.np.nan)
            df[col] = df[col].astype(object)

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
            "mun_id":                               "UInt16",
            "year":                                 "UInt16",
            "population":                           "UInt32",
            "poverty":                              "UInt32",
            "extreme_poverty":                      "UInt32",
            "moderate_poverty":                     "UInt32",
            "vulnerable_lacks":                     "UInt32",
            "vulnerable_income":                    "UInt32",
            "no_vulnerable":                        "UInt32",
            "educational_backwardness":             "UInt32",
            "deprivation_quality_housing_spaces":   "UInt32",
            "deprivation_health_services":          "UInt32",
            "deprivation_social_security":          "UInt32",
            "deprivation_basic_services_housing":   "UInt32",
            "deprivation_food_access":              "UInt32",
            "at_least_one_lack":                    "UInt32",
            "at_least_three_lacks":                 "UInt32",
            "income_below_welfare_line":            "UInt32",
            "income_below_min_welfare_line":        "UInt32"
        }

        download_step = DownloadStep(
            connector="poverty-data",
            connector_path="conns.yaml"
        )
        transform_step = TransformStep()
        load_step = LoadStep(
            "coneval_poverty", db_connector, if_exists="append", pk=["mun_id", "year"], dtype=dtype, 
            nullable_list=[
                "population", "poverty", "extreme_poverty", "moderate_poverty", "vulnerable_lacks", 
                "vulnerable_income", "no_vulnerable", "educational_backwardness", "deprivation_quality_housing_spaces", 
                "deprivation_health_services", "deprivation_social_security", "deprivation_basic_services_housing", 
                "deprivation_food_access", "at_least_one_lack", "at_least_three_lacks", "income_below_welfare_line", 
                "income_below_min_welfare_line"
            ]
        )

        return [download_step, transform_step, load_step]