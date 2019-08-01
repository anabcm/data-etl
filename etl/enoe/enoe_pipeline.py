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
        excel_url = "https://docs.google.com/spreadsheets/d/e/2PACX-1vSg-NM8Jt_vHnuIcJ3fjHMxcae_IkK7sresHvhUs_G7NSM5CN5NGYiCf-BP_GMPw3jwmm791CXPLpqJ/pub?output=xlsx"
        df_labels = pd.ExcelFile(excel_url)

        # Loading 2 ENOE files, in order to create 1 quarter per year data
        try:
            dt_1 = pd.read_csv(prev[0], index_col=None, header=0, encoding="latin-1", dtype=str, 
                                usecols =[
                "ent", "eda", "p1b", "p2_1", "p2_2", "p2_3", "p2_4", "p2_9", "p2a_anio", "p2b", "p3",
                "p4a", "p5b_thrs", "p5b_tdia", "fac", "con", "upm", "n_pro_viv", "v_sel"])
        except:
            try:
                dt_1 = pd.read_csv(prev[0], index_col=None, header=0, encoding="latin-1", dtype=str, 
                                    usecols=[
                    "ent", "eda", "p1b", "p2_1", "p2_2", "p2_3", "p2_4", "p2_9", "p2a_anio", "p2b", "p3",
                    "p4a", "p5c_thrs", "p5c_tdia", "fac", "con", "upm", "n_pro_viv", "v_sel"])
                dt_1.rename(index=str, columns={
                                    "p5c_thrs": "p5b_thrs",
                                    "p5c_tdia": "p5b_tdia"}, inplace=True)
            except:
                dt_1 = pd.read_csv(prev[0], index_col=None, header=0, encoding="latin-1", dtype=str, 
                                    usecols = lambda x: x.lower() in [
                    "ent", "eda", "p1b", "p2_1", "p2_2", "p2_3", "p2_4", "p2_9", "p2a_anio", "p2b", "p3",
                    "p4a", "p5b_thrs", "p5b_tdia", "fac", "con", "upm", "n_pro_viv", "v_sel"])

        dt_2 = pd.read_csv(prev[1], index_col=None, header=0, encoding="latin-1", dtype=str,
        usecols= lambda x: x.lower() in ["ent", "con", "upm", "n_pro_viv", "v_sel", "p6b1",
        "p6b2", "p6c", "p6d", "p7", "p7a", "p7c"])

        # Standarizing headers, some files are capitalized
        dt_1.columns = dt_1.columns.str.lower()
        dt_2.columns = dt_2.columns.str.lower()

        # Creating df
        dt_1["code"] = dt_1["ent"] + dt_1["con"] + dt_1["upm"] + dt_1["v_sel"] + dt_1["n_pro_viv"]
        dt_2["code"] = dt_2["ent"] + dt_2["con"] + dt_2["upm"] + dt_2["v_sel"] + dt_2["n_pro_viv"]

        df = pd.merge(dt_1, dt_2[["code", "p6b1", "p6b2", "p6c", "p6d", "p7", "p7a", "p7c"]], on="code", how="left")

        # Getting values of year and respective quarter for the survey
        df["time"] = params["year"] + params["quarter"]
        df["time"] = df["time"].astype(int)

        # Dictionaries for renaming the columns
        part1 = pd.read_excel(df_labels, "part1")
        part2 = pd.read_excel(df_labels, "part2")

        # Renaming of the columns for a explanatory ones
        df.rename(columns = dict(zip(part1.column, part1.new_column)), inplace=True)
        df.rename(columns = dict(zip(part2.column, part2.new_column)), inplace=True)

        # Loading table with mun and loc values
        housing = pd.read_csv(prev[2], index_col=None, header=0, encoding="latin-1", dtype=str, 
                        usecols= lambda x: x.lower() in["ent", "con", "upm", "v_sel", "n_pro_viv", "mun", "loc"])
        housing.columns = housing.columns.str.lower()

        # Creating an unique value to compare between dfs
        df["code"] = df["ent_id"] + df["con"] + df["upm"] + df["v_sel"]+ df["numero_vivienda"]
        housing["code"] = housing["ent"] + housing["con"] + housing["upm"] + housing["v_sel"] + housing["n_pro_viv"]

        # Keeping just the needed values from vivienda, and merge them into the df
        _list = ["code", "loc", "mun"]
        housing = housing[_list]
        df = df.merge(housing, on="code", how="left")

        # Creating news geo ids, and deleting another values
        df["loc_id"] = df["ent_id"] + df["mun"] + df["loc"]
        list_drop = ["con", "upm", "v_sel", "numero_vivienda", "code" , "mun", "loc", "ent_id"]
        df.drop(list_drop, axis=1, inplace=True)

        # Replacing NaN an empty values in order to change content of the columns with IDs
        df.replace(pd.np.nan, 99999, inplace=True)
        df.replace(" ", 99999, inplace=True)

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

        # Turning back NaN values in the respective columns
        df.replace(99999, pd.np.nan, inplace=True)
        df["actual_job_days_worked_lastweek"].replace("9", pd.np.nan, inplace=True)

        for col in ["has_job_or_business", "search_job_overseas", "search_job_mexico", "search_start_business",
                    "search_no_search", "search_job_year", "time_looking_job", "actual_job_position", "actual_job_industry_group_id",
                    "actual_job_hrs_worked_lastweek", "actual_job_days_worked_lastweek", 
                    "actual_frecuency_payments", "actual_amount_pesos", "actual_minimal_wages_proportion", "actual_healthcare_attention",
                   "second_activity", "second_activity_task", "second_activity_group_id"]:
            df[col] = df[col].astype(float)

        for item in ["age", "loc_id", "population"]:
            df[item] = df[item].astype(int)

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
            "loc_id":                               "UInt32",
            "time":                                 "UInt8",
            "age":                                  "UInt8",
            "has_job_or_business":                  "UInt8",
            "search_job_overseas":                  "UInt8",
            "search_job_mexico":                    "UInt8",
            "search_start_business":                "UInt8",
            "search_no_search":                     "UInt8",
            "search_no_knowledge":                  "UInt8",
            "search_job_year":                      "UInt8",
            "time_looking_job":                     "UInt8",
            "actual_job_position":                  "UInt16",
            "actual_job_industry_group_id":         "UInt16",
            "actual_job_hrs_worked_lastweek":       "UInt8",
            "actual_job_days_worked_lastweek":      "UInt8",
            "population":                           "UInt64", 
            "actual_frecuency_payments":            "UInt8",
            "actual_amount_pesos":                  "UInt32",
            "actual_minimal_wages_proportion":      "UInt8",
            "actual_healthcare_attention":          "UInt8",
            "second_activity":                      "UInt8",
            "second_activity_task":                 "UInt16",
            "second_activity_group_id":             "UInt16"
        }

        download_step = DownloadStep(
            connector=["enoe-1-data", "enoe-2-data", "housing-data"],
            connector_path="conns.yaml"
        )
        transform_step = TransformStep()
        load_step = LoadStep(
            "inegi_enoe", db_connector, if_exists="append", pk=["loc_id", "time"], dtype=dtype, 
            nullable_list=[
              "search_job_year", "actual_job_position", "actual_job_industry_group_id", "actual_job_hrs_worked_lastweek",
              "actual_amount_pesos", "second_activity_task", "second_activity_group_id", "second_activity","actual_healthcare_attention", 
              "has_job_or_business", "search_job_overseas", "search_job_mexico", "search_start_business", "search_no_search", 
              "search_no_knowledge", "time_looking_job", "actual_job_days_worked_lastweek", "actual_frecuency_payments",
              "actual_minimal_wages_proportion"]
        )

        return [download_step, transform_step, load_step]