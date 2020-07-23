import numpy as np
import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import DownloadStep
from bamboo_lib.steps import LoadStep


class TransformStep(PipelineStep):
    def run_step(self, prev, params):

        excel_url = "https://docs.google.com/spreadsheets/d/e/2PACX-1vSg-NM8Jt_vHnuIcJ3fjHMxcae_IkK7sresHvhUs_G7NSM5CN5NGYiCf-BP_GMPw3jwmm791CXPLpqJ/pub?output=xlsx"
        df_labels = pd.ExcelFile(excel_url)

        # Loading 2 ENOE files, in order to create 1 quarter per year data || New loading step
        cols = [["ent", "cd_a", "con", "v_sel", "n_hog", "h_mud", "n_ren", "eda", "p1b", "p2_1", "p2_2", "p2_3", "p2_4", "p2_9", "p2a_anio", "p2b", "p3", "p4a", "p5b_thrs", "p5b_tdia", "fac"],
                ["ent", "cd_a", "con", "v_sel", "n_hog", "h_mud", "n_ren", "eda", "p1b", "p2_1", "p2_2", "p2_3", "p2_4", "p2_9", "p2a_anio", "p2b", "p3", "p4a", "p5c_thrs", "p5c_tdia", "fac"]]

        def upper_(array):
            return [x.upper() for x in array]
        def lower_(array):
            return [x.lower() for x in array]

        # ENOE COE1T
        for col in cols:
            for op in [upper_, lower_]:
                try:
                    dt_1 = pd.read_csv(prev[0], index_col=None, header=0, encoding="latin-1", dtype=str, 
                                usecols=op(col))
                    break
                except:
                    continue

        # ENOE COE2T
        dt_2 = pd.read_csv(prev[1], index_col=None, header=0, encoding="latin-1", dtype=str,
        usecols= lambda x: x.lower() in ["ent", "con", "v_sel", "n_hog", "h_mud", "n_ren", "p6b1", "p6b2", "p6c", "p6d", "p7", "p7a", "p7c"])

        # Standarizing headers, some files are capitalized
        dt_1.columns = dt_1.columns.str.lower()
        dt_2.columns = dt_2.columns.str.lower()

        # Renaming columns from first quarter to match the rest of the year
        dt_1.rename(index=str, columns={"p5c_thrs": "p5b_thrs","p5c_tdia": "p5b_tdia"}, inplace=True)

        # Creating df, based in unique individual values (prevent overpopulation with merge)
        dt_1["code"] = dt_1["ent"] + dt_1["con"] + dt_1["v_sel"] + dt_1["n_hog"] + dt_1["h_mud"] + dt_1["n_ren"]
        dt_2["code"] = dt_2["ent"] + dt_2["con"] + dt_2["v_sel"] + dt_2["n_hog"] + dt_2["h_mud"] + dt_2["n_ren"]

        df = pd.merge(dt_1, dt_2[["code", "p6b1", "p6b2", "p6c", "p6d", "p7", "p7a", "p7c"]], on="code", how="left")

        # Loading social-demographic table, adding gender and active/inactive economic population
        # ENOE SDEMT
        social_ = pd.read_csv(prev[2], index_col=None, header=0, encoding="latin-1", dtype=str,
                        usecols= lambda x: x.lower() in ["ent", "con", "v_sel", "n_hog", "h_mud", "n_ren", 
                                "sex", "clase1", "clase2", "clase3", "ma48me1sm", "hij5c", "anios_esc", 
                                "cs_p13_1", "cs_p13_2", "ingocup", "d_ant_lab", "d_cexp_est", "dur_des", 
                                "sub_o", "s_clasifi", "cp_anoc", "emp_ppal"])
        social_.columns = social_.columns.str.lower()

        # Creating same code value to identified individual values
        social_["code"] = social_["ent"] + social_["con"] + social_["v_sel"] + social_["n_hog"] + social_["h_mud"] + social_["n_ren"]

        # Merging just the needed column from social-demographic
        df = pd.merge(df, social_[["code", "sex", "clase1", "clase2", "clase3", "ma48me1sm", "hij5c", "anios_esc", 
                                "cs_p13_1", "cs_p13_2", "ingocup", "d_ant_lab", "d_cexp_est", "dur_des", 
                                "sub_o", "s_clasifi", "cp_anoc", "emp_ppal"]], on="code", how="left")

        # Dictionaries for renaming the columns
        part1 = pd.read_excel(df_labels, "part1")
        part2 = pd.read_excel(df_labels, "part2")

        # Renaming of the columns for a explanatory ones
        df.rename(columns = dict(zip(part1.column, part1.new_column)), inplace=True)
        df.rename(columns = dict(zip(part2.column, part2.new_column)), inplace=True)

        # Loading table with mun and loc values
        # ENOE VIVT
        housing = pd.read_csv(prev[3], index_col=None, header=0, encoding="latin-1", dtype=str, 
                        usecols= lambda x: x.lower() in ["ent", "con", "v_sel", "mun"])
        housing.columns = housing.columns.str.lower()

        # 2020 data format change
        housing["mun"] = housing["mun"].str.zfill(3)
        housing["ent"] = housing["ent"].str.zfill(2)
        housing["v_sel"] = housing["v_sel"].str.zfill(2)

        # Filling with 0s "ent_id" and "v_sel" given 2019 second quarter issue
        df["ent_id"] = df["ent_id"].str.zfill(2)
        df["v_sel"] = df["v_sel"].str.zfill(2)

        # Creating an unique value to compare between dfs
        df["code"] = df["ent_id"] + df["con"] + df["v_sel"]
        housing["code"] = housing["ent"] + housing["con"] + housing["v_sel"]

        # Merging just the needed column from vivienda
        df = pd.merge(df, housing[["code", "mun"]], on="code", how="left")

        # Creating news geo ids, and deleting another values
        df["mun_id"] = df["ent_id"] + df["mun"]
        list_drop = ["ent_id", "con", "v_sel", "n_hog", "h_mud", "numero_renglon", "mun"]
        df.drop(list_drop, axis=1, inplace=True)

        # Replacing NaN an empty values in order to change content of the columns with IDs
        df.replace(pd.np.nan, 999999, inplace=True)
        df.replace(" ", 999999, inplace=True)

        # Changing columns with IDs trought cycle
        filling = ["has_job_or_business", "search_job_overseas", "search_job_mexico",
                    "search_start_business", "search_no_search", "search_no_knowledge",
                    "actual_frecuency_payments", "actual_minimal_wages_proportion", 
                    "actual_healthcare_attention", "second_activity", "time_looking_job"]

        # For cycle in order to change the content of a column from previous id, into the new ones (working for translate too)
        for sheet in filling:
            df_l = pd.read_excel(df_labels, sheet)
            df[sheet] = df[sheet].astype(float)
            df[sheet] = df[sheet].replace(dict(zip(df_l.prev_id, df_l.id)))

        df["population"] = df["population"].astype(int)

        # Loading income values from spreedsheet and income_id column
        url = "https://docs.google.com/spreadsheets/d/e/2PACX-1vTieVnRovfP7AOMtqxIJcrFl8Tayz6Irz-Bc1en1NSIKtjjPtGaBRaCaSeePRrpQMmHMzSt2VO93Wav/pub?output=xlsx"
        pivote = pd.read_excel(url, sheet_name="Sheet1", encoding="latin-1", dtype={"interval_upper": "int64", "interval_lower": "int64"})
        df["income_id"] = df["actual_amount_pesos"].astype(int)

        # Transforming income_id values to actual IDs 
        for pesos in df.income_id.unique():
            for level in range(pivote.shape[0]):
                if (pesos >= pivote.interval_lower[level]) & (pesos < pivote.interval_upper[level]):
                    df.income_id.replace(pesos, str(pivote.id[level]), inplace=True)
                    break
        df.income_id = df.income_id.astype("int")

        # Turning back NaN values in the respective columns
        df.replace(999999, pd.np.nan, inplace=True)
        df["actual_job_days_worked_lastweek"].replace("9", pd.np.nan, inplace=True)
        df["actual_job_hrs_worked_lastweek"].replace("999", pd.np.nan, inplace=True)
        df["income_id"].replace(99, pd.np.nan, inplace=True)

        #Setting types
        for col in ["has_job_or_business", "search_job_overseas", "search_job_mexico", "search_start_business",
                    "search_no_search", "search_job_year", "time_looking_job", "actual_job_position", "actual_job_industry_group_id",
                    "actual_job_hrs_worked_lastweek", "actual_job_days_worked_lastweek", "represented_city",
                    "actual_frecuency_payments", "actual_amount_pesos", "actual_minimal_wages_proportion", "actual_healthcare_attention",
                    "second_activity", "second_activity_task", "second_activity_group_id", "income_id", "sex", "eap",
                    "occ_unocc_pop", "eap_comp", "_48hrs_less_1", "female_15yrs_children", "schooling_years", "approved_years", 
                    "instruction_level", "mensual_wage", "work_history", "unoccupied_condition", "classification_duration_unemployment",
                    "underemployed_population", "underemployed_classification", "classification_self_employed_unqualified_activities",
                    "classification_formal_informal_jobs_first_activity"]:
            df[col] = df[col].astype(float)

        for item in ["age", "mun_id"]:
            df.dropna(subset=[item], inplace=True)
            df[item] = df[item].astype(int)

        # Turning small comunities ids to NaN values
        df["represented_city"].replace([81, 82, 83, 84, 85, 86], pd.np.nan, inplace=True)

        # Filter population for 15 and/or older
        df = df.loc[(df["age"] >= 15)].reset_index(col_fill="ffill", drop=True)

        # Getting values of year and respective quarter for the survey
        df = df[["code", "population", "mensual_wage", "has_job_or_business", 
                 "second_activity", "eap", "occ_unocc_pop", "eap_comp",
                 "schooling_years", "instruction_level", "approved_years",
                 "classification_formal_informal_jobs_first_activity", "age",
                 "actual_job_industry_group_id", "sex",  "actual_job_position",
                 "actual_job_hrs_worked_lastweek", "actual_job_days_worked_lastweek"]].copy()

        # filters economic active population, 0 = undefined
        df = df.loc[df["eap"] != 0].copy()

        # cut for non null values
        df["workforce_is_wage"] = df["population"]
        df.loc[df["mensual_wage"].isna(), "workforce_is_wage"] = 0

        df["code"] = df["code"].astype(int)
        df["quarter_id"] = "20" + params["year"] + params["quarter"]
        df["quarter_id"] = df["quarter_id"].astype(int)

        return df

class ENOEPipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return [
            Parameter(label="Year", name="year", dtype=str),
            Parameter(label="Quarter", name="quarter", dtype=str)
        ]

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch("clickhouse-database", open("../conns.yaml"))

        dtype = {
            "code":                                                 "UInt32",
            "population":                                           "UInt32",
            "mensual_wage":                                         "Float32",
            "quarter_id":                                           "UInt16",
            "has_job_or_business":                                  "UInt8",
            "second_activity":                                      "UInt8",
            "eap":                                                  "UInt8",
            "occ_unocc_pop":                                        "UInt8",
            "eap_comp":                                             "UInt8",
            "schooling_years":                                      "UInt8",
            "approved_years":                                       "UInt8",
            "instruction_level":                                    "UInt8",
            "classification_formal_informal_jobs_first_activity":   "UInt8",
            "age":                                                  "UInt8",
            "actual_job_industry_group_id":                         "UInt16",
            "sex":                                                  "UInt8",
            "actual_job_position":                                  "UInt16",
            "actual_job_hrs_worked_lastweek":                       "UInt8",
            "actual_job_days_worked_lastweek":                      "UInt8",
            "workforce_is_wage":                                    "UInt32"
        }

        download_step = DownloadStep(
            connector=["enoe-1-data", "enoe-2-data", "social-data", "housing-data"],
            connector_path="conns.yaml"
        )
        transform_step = TransformStep()
        load_step = LoadStep(
            "inegi_enoe", db_connector, if_exists="append", pk=["code"], dtype=dtype,
            nullable_list=["actual_job_hrs_worked_lastweek", "actual_job_days_worked_lastweek", "mensual_wage",
                           "has_job_or_business", "actual_job_position", "sex", "actual_job_industry_group_id",
                           "eap_comp", "occ_unocc_pop", "eap", "second_activity"]
        )

        return [download_step, transform_step, load_step]