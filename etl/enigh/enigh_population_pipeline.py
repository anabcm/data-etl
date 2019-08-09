import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import DownloadStep
from bamboo_lib.steps import LoadStep


class TransformStep(PipelineStep):
    def run_step(self, prev, params):

        # Loading labels from spredsheet
        excel_url = "https://docs.google.com/spreadsheets/d/e/2PACX-1vQBERMw0WN6G1w1keYXtu6mF22nbR2VnqIi91PieJCgJcyu6WCkqD-mLSpYxpueWAx0145SDhYbmUII/pub?output=xlsx"
        df_labels = pd.ExcelFile(excel_url)


        # List to cloumns to bring from population
        list_cols = ["folioviv", "foliohog", "sexo", "edad", "hablaind", "etnia", "nivelaprob", "residencia", "segsoc",
        "ss_aa", "ss_mm", "segsoc", "redsoc_1", "redsoc_2", "redsoc_3", "redsoc_4", "redsoc_5", "redsoc_6", "hor_1",
        "segpop", "atemed", "inst_1", "inst_2", "inst_3", "inst_4", "inst_5", "inst_6", "hh_lug", "mm_lug", "hh_esp",
        "mm_esp", "trabajo_mp", "motivo_aus", "act_pnea1", "act_pnea2", "num_trabaj"]

        # Loading population file
        dt_1 = pd.read_csv(prev[0], index_col=None, header=0, encoding="latin-1", dtype=str, usecols = list_cols)

        # Time in social security
        dt_1["ss_aa"].replace("-1", 999999, inplace = True)
        dt_1["ss_mm"].replace("-1", 999999, inplace = True)

        # Time related to health attention
        for item in ["hh_lug", "mm_lug", "hh_esp", "mm_esp"]:
            dt_1[item].replace(" ", "0", inplace=True)
            dt_1[item] = dt_1[item].astype(int)

        dt_1["near_healthcare_center"] = dt_1["hh_lug"]*60 + dt_1["mm_lug"]
        dt_1["waiting_health_attention"] = dt_1["hh_esp"]*60 + dt_1["mm_esp"]

        # Filling empty cells
        dt_1.replace(" ", 999999, inplace = True)

        # Loading enigh housing dataframe in order to get mun_id and factor columns
        df_viv = pd.read_csv(prev[1], index_col=None, header=0, encoding="latin-1", dtype=str,
                usecols= ["folioviv", "ubica_geo", "est_socio", "factor"])

        df = pd.merge(dt_1, df_viv[["folioviv", "ubica_geo", "est_socio", "factor"]],
                    on="folioviv", how="left")

        df["mun_id"] = df["ubica_geo"].str.slice(0,5)

        # Changing columns with IDs trought cycle
        filling = ["sexo", "hablaind", "etnia", "nivelaprob", "residencia", "segsoc", "redsoc_1",
            "redsoc_2", "redsoc_3", "redsoc_4", "redsoc_5", "redsoc_6", "segpop",
            "atemed", "inst_1", "inst_2", "inst_3", "inst_4", "inst_5", "inst_6",
            "trabajo_mp", "motivo_aus", "act_pnea1", "act_pnea2", "num_trabaj", "est_socio"]

        # For cycle in order to change the content of a column from previous id, into the new ones (working for translate too)
        for sheet in filling:
            df_l = pd.read_excel(df_labels, sheet)
            df[sheet] = df[sheet].astype(float)
            df[sheet] = df[sheet].replace(dict(zip(df_l.prev_id, df_l.id)))

        # Droping already used columns
        list_drop = ["hh_lug", "mm_lug", "hh_esp", "mm_esp", "ubica_geo", "folioviv", "foliohog", "ubica_geo"]
        df.drop(list_drop, axis=1, inplace=True)

        # Turning back NaN values in the respective columns
        df.replace(999999, pd.np.nan, inplace = True)

        # Renaming the columns to english
        params = {
            "sexo": "sex", 
            "edad": "age",
            "hablaind": "speaks_native",
            "etnia": "etnicity",
            "residencia": "reference_city",
            "segsoc": "social_security",
            "ss_aa": "social_security_years",
            "ss_mm": "social_security_months",
            "redsoc_1": "near_support_money",
            "redsoc_2": "near_support_sickness",
            "redsoc_3": "near_support_work",
            "redsoc_4": "near_support_doctor",
            "redsoc_5": "near_support_neighborhood",
            "redsoc_6": "near_support_childrens",
            "nivelaprob": "academic_degree",
            "hor_1": "working_hours",
            "segpop": "popular_insurance",
            "atemed": "health_attention",
            "trabajo_mp": "work_last_month",
            "num_trabaj": "number_jobs",
            "motivo_aus": "job_absence",
            "est_socio": "eco_stratum",
            "factor": "population"}

        df.rename(index=str, columns=params, inplace=True)

        df["population"] = df["population"].astype(int)
        df["working_hours"].replace(999999, 0, inplace = True)

        group_list = ["sex", "age", "speaks_native", "etnicity", "academic_degree", "reference_city",
        "social_security", "social_security_years", "social_security_months", "near_support_money",
        "near_support_sickness", "near_support_work", "near_support_doctor", "near_support_neighborhood",
        "near_support_childrens", "popular_insurance", "health_attention", "working_hours",
        "inst_1", "inst_2", "inst_3", "inst_4", "inst_5", "inst_6", "work_last_month", "job_absence",
        "act_pnea1", "act_pnea2", "number_jobs", "eco_stratum", "mun_id", "near_healthcare_center",
        "waiting_health_attention"]

        df = df.groupby(group_list).sum().reset_index(col_fill="ffill")

        df.replace(999999, pd.np.nan, inplace = True)
        df["working_hours"].replace(0, pd.np.nan, inplace = True)

        # Changing types for certains columns
        null_list = ["speaks_native", "etnicity", "academic_degree", "reference_city", "social_security",
            "social_security_years", "social_security_months", "near_support_money", "near_support_sickness",
            "near_support_work", "near_support_doctor", "near_support_neighborhood", "near_support_childrens",
            "working_hours", "popular_insurance", "health_attention", "inst_1", "inst_2", "inst_3", "inst_4",
            "inst_5", "inst_6", "work_last_month", "job_absence", "act_pnea1", "act_pnea2", "number_jobs",
            "near_healthcare_center", "waiting_health_attention"]

        for col in null_list:
            df[col] = df[col].astype(float)

        for item in ["mun_id", "population", "sex", "age"]:
            df[item] = df[item].astype(int)

        return df

class EnighPopulationPipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return []

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch("clickhouse-database", open("../conns.yaml"))

        dtype = {
            "speaks_native":                   "UInt8",
            "etnicity":                        "UInt8",
            "academic_degree":                 "UInt8",
            "reference_city":                  "UInt8",
            "social_security":                 "UInt8",
            "social_security_years":           "UInt8",
            "social_security_months":          "UInt8",
            "near_support_money":              "UInt8",
            "near_support_sickness":           "UInt8",
            "near_support_work":               "UInt8",
            "near_support_doctor":             "UInt8",
            "near_support_neighborhood":       "UInt8",
            "near_support_childrens":          "UInt8",
            "working_hours":                   "UInt16",
            "popular_insurance":               "UInt8",
            "health_attention":                "UInt8",
            "inst_1":                          "UInt8",
            "inst_2":                          "UInt8",
            "inst_3":                          "UInt8",
            "inst_4":                          "UInt8",
            "inst_5":                          "UInt8",
            "inst_6":                          "UInt8",
            "work_last_month":                 "UInt8",
            "job_absence":                     "UInt8",
            "act_pnea1":                       "UInt8",
            "act_pnea2":                       "UInt8",
            "number_jobs":                     "UInt8",
            "mun_id":                          "UInt16",
            "population":                      "UInt16",
            "eco_stratum":                     "UInt8",
            "sex":                             "UInt8",
            "age":                             "UInt8"
        }

        download_step = DownloadStep(
            connector=["enigh-population", "enigh-housing"],
            connector_path="conns.yaml"
        )
        transform_step = TransformStep()
        load_step = LoadStep(
            "inegi_enigh_population", db_connector, if_exists="append", pk=["mun_id", "sex"], dtype=dtype, 
            nullable_list=["speaks_native", "etnicity", "academic_degree", "reference_city", "social_security",
            "social_security_years", "social_security_months", "near_support_money", "near_support_sickness",
            "near_support_work", "near_support_doctor", "near_support_neighborhood", "near_support_childrens",
            "working_hours", "popular_insurance", "health_attention", "inst_1", "inst_2", "inst_3", "inst_4",
            "inst_5", "inst_6", "work_last_month", "job_absence", "act_pnea1", "act_pnea2", "number_jobs"]
        )

        return [download_step, transform_step, load_step]