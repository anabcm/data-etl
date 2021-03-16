
import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import Parameter, EasyPipeline, PipelineStep
from bamboo_lib.steps import DownloadStep, LoadStep


class TransformStep(PipelineStep):
    def run_step(self, prev, params):

        # Loading labels from spredsheet
        df_labels = pd.ExcelFile(prev[2])

        # List to columns to bring from population file
        list_cols = ["folioviv", "foliohog", "sexo", "edad", "hablaind", "etnia", "nivelaprob", "residencia", "segsoc", "ss_aa", "ss_mm",
                "segsoc", "redsoc_1", "redsoc_2", "redsoc_3", "redsoc_4", "redsoc_5", "redsoc_6", "hor_1", "segpop",
                "atemed", "inst_1", "inst_2", "inst_3", "inst_4", "inst_5", "inst_6", "hh_lug", "mm_lug", "hh_esp", "mm_esp",
                "trabajo_mp", "motivo_aus", "act_pnea1", "act_pnea2", "num_trabaj"]

        # Loading population and household files
        df_1 = pd.read_csv(prev[0], index_col=None, header=0, encoding="latin-1", dtype=str, usecols = list_cols)
        df_2 = pd.read_csv(prev[1], index_col=None, header=0, encoding="latin-1", dtype=str,
                usecols=["folioviv", "foliohog", "ubica_geo", "factor", "est_socio"])

        # Creating unique code column for the merge method
        df_1["code"] = df_1["folioviv"].astype("str") + df_1["foliohog"].astype("str").str.zfill(3)
        df_2["code"] = df_2["folioviv"].astype("str") + df_2["foliohog"].astype("str").str.zfill(3)

        # Merge method to add population, mun_id and social stratus columns
        df = pd.merge(df_1,  df_2[["code", "ubica_geo", "est_socio", "factor"]], on="code", how="left")

        # Creating mun_id from geo ubication column
        df["mun_id"] = df["ubica_geo"].str.slice(0,5)

        # Time related to health attention to int
        for item in ["hh_lug", "mm_lug", "hh_esp", "mm_esp"]:
            df[item].replace(" ", "0", inplace=True)
            df[item] = df[item].astype(int)

        # Time related to health attention
        df["near_healthcare_center"] = df["hh_lug"]*60 + df["mm_lug"]
        df["waiting_health_attention"] = df["hh_esp"]*60 + df["mm_esp"]

        # Columns related to social security
        df["ss_aa"].replace("-1", 0, inplace = True)
        df["ss_mm"].replace("-1", 0, inplace = True)
        df["ss_aa"].replace(" ", 0, inplace = True)
        df["ss_mm"].replace(" ", 0, inplace = True)

        # Months with social security
        df["months_social_security"] = df["ss_aa"].astype(int) * 12 + df["ss_mm"].astype(int)

        # Turning to NaN the people that is under age, or have not signed for SS
        df.loc[df["segsoc"] == " ", "months_social_security"] = pd.np.nan
        df.loc[df["segsoc"] == "2", "months_social_security"] = pd.np.nan

        # Replacing empty values to pass the groupby method
        df.replace(" ", 999999, inplace = True)

        # Changing columns with IDs trought cycle
        filling = ["sexo", "hablaind", "etnia", "nivelaprob", "residencia", "segsoc", "redsoc_1",
                    "redsoc_2", "redsoc_3", "redsoc_4", "redsoc_5", "redsoc_6", "segpop",
                    "atemed", "inst_1", "inst_2", "inst_3", "inst_4", "inst_5", "inst_6",
                    "trabajo_mp", "motivo_aus", "act_pnea1", "act_pnea2", "num_trabaj", "est_socio"]

        for sheet in filling:
            df_l = pd.read_excel(df_labels, sheet)
            df[sheet] = df[sheet].astype(int)
            df[sheet] = df[sheet].replace(dict(zip(df_l.prev_id, df_l.id)))

        # Renaming step
        params_naming = {
                "sexo": "sex",
                "edad": "age",
                "hablaind": "speaks_native",
                "etnia": "ethnicity",
                "residencia": "previous_entity_5_years",
                "segsoc": "social_security",
                "redsoc_1": "near_support_money",
                "redsoc_2": "near_support_sickness",
                "redsoc_3": "near_support_work",
                "redsoc_4": "near_support_doctor",
                "redsoc_5": "near_support_neighborhood",
                "redsoc_6": "near_support_children",
                "nivelaprob": "academic_degree",
                "hor_1": "worked_hours_last_week",
                "segpop": "popular_insurance",
                "atemed": "health_attention",
                "trabajo_mp": "work_last_month",
                "num_trabaj": "number_jobs",
                "motivo_aus": "job_absence",
                "est_socio": "eco_stratum",
                "tam_loc": "loc_size",
                "factor": "population"}

        df.rename(index=str, columns=params_naming, inplace=True)

        # Deleting already used columns
        list_drop = ["folioviv", "foliohog", "ss_aa", "ss_mm", "hh_lug", "mm_lug", "hh_esp", "mm_esp", "code", "ubica_geo"]
        df.drop(list_drop, axis=1, inplace=True)

        # Grouping columns
        group_list = ["mun_id", "sex", "age", "speaks_native", "ethnicity", "academic_degree", "previous_entity_5_years",
                    "social_security", "months_social_security", "near_support_money",
                    "near_support_sickness", "near_support_work", "near_support_doctor", "near_support_neighborhood",
                    "near_support_children", "worked_hours_last_week", "popular_insurance", "health_attention", "work_last_month",
                    "inst_1", "inst_2", "inst_3", "inst_4", "inst_5", "inst_6", "job_absence",
                    "act_pnea1", "act_pnea2", "number_jobs", "eco_stratum", "near_healthcare_center",
                    "waiting_health_attention"]

        # Setting population to int in order to sum()
        df["population"] = df["population"].astype(int)

        # Groupby method
        df = df.groupby(group_list).sum().reset_index(col_fill="ffill")

        # Turning back the NaN values to the df
        df.replace(999999, pd.np.nan, inplace = True)

        # Adding respective year to the Dataframes, given Inegis update (2016-2018)
        df["year"] = params["year"]

        # Changing types for certains columns
        null_list = ["near_support_money", "near_support_sickness", "near_support_work", "near_support_doctor",
            "near_support_neighborhood", "near_support_children", "worked_hours_last_week", "inst_1", "inst_2", "inst_3",
            "inst_4", "inst_5", "inst_6", "job_absence", "act_pnea1", "act_pnea2", "number_jobs", "near_healthcare_center",
            "waiting_health_attention", "year"]

        non_null_list = ["mun_id", "sex", "age", "speaks_native", "ethnicity", "academic_degree", "previous_entity_5_years",
            "social_security", "months_social_security", "popular_insurance", "health_attention", "work_last_month", "eco_stratum",
            "population"]

        for col in null_list:
            df[col] = df[col].astype(float)

        for item in non_null_list:
            df[item] = df[item].astype(int)

        return df

class EnighPopulationPipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return [
            Parameter(label="Year", name="year", dtype=str)
        ]

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch("clickhouse-database", open("../conns.yaml"))

        dtype = {
            "speaks_native":                   "UInt8",
            "ethnicity":                       "UInt8",
            "academic_degree":                 "UInt8",
            "previous_entity_5_years":         "UInt8",
            "social_security":                 "UInt8",
            "months_social_security":          "UInt8",
            "near_support_money":              "UInt8",
            "near_support_sickness":           "UInt8",
            "near_support_work":               "UInt8",
            "near_support_doctor":             "UInt8",
            "near_support_neighborhood":       "UInt8",
            "near_support_children":           "UInt8",
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
            "age":                             "UInt8",
            "year":                            "UInt16"
        }

        download_step = DownloadStep(
            connector=["enigh-population", "enigh-household", "enigh-poppulation-expenses"],
            connector_path="conns.yaml"
        )
        transform_step = TransformStep()
        load_step = LoadStep(
            "inegi_enigh_population", db_connector, if_exists="append", pk=["mun_id", "sex"], dtype=dtype, 
            nullable_list=["near_support_money", "near_support_sickness", "near_support_work", "near_support_doctor",
            "near_support_neighborhood", "near_support_children", "worked_hours_last_week", "inst_1", "inst_2", "inst_3",
            "inst_4", "inst_5", "inst_6", "job_absence", "act_pnea1", "act_pnea2", "number_jobs"]
        )

        return [download_step, transform_step, load_step]