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
            dt_1 = pd.read_csv(prev[0], index_col=None, header=0, encoding="latin-1",
            usecols = ["ent", "eda", "p1b", "p2_1", "p2_2", "p2_3", "p2_4",
            "p2_9", "p2a_anio", "p2b", "p3",
            "p4a", "p5b_thrs", "p5b_tdia", "fac"])
        except:
            dt_1 = pd.read_csv(prev[0], index_col=None, header=0, encoding="latin-1",
            usecols = lambda x: x.lower() in ["ent", "eda", "p1b", "p2_1", "p2_2", "p2_3", "p2_4",
            "p2_9", "p2a_anio", "p2b", "p3","p4a", "p5c_thrs", "p5c_tdia", "fac"])

        dt_2 = pd.read_csv(prev[1], index_col=None, header=0, encoding="latin-1",
        usecols= lambda x: x.lower() in ["p6b1", "p6b2", "p6c", "p6d", "p7", "p7a", "p7c"])

        # Standarizing headers, some files are capitalized
        dt_1.columns = dt_1.columns.str.lower()
        dt_2.columns = dt_2.columns.str.lower()

        # Creating df
        df = dt_1.join(dt_2)

        # Getting values of year and respective quarter for the survey
        df["time"] = params["year"] + params["quarter"]
        df["time"] = df["time"].astype(int)

        # Dictionaries for renaming the columns
        part1 = pd.read_excel(df_labels, "part1")
        part2 = pd.read_excel(df_labels, "part2")

        # Renaming of the columns for a explanatory ones
        df.rename(columns = dict(zip(part1.column, part1.new_column)), inplace=True)
        df.rename(columns = dict(zip(part2.column, part2.new_column)), inplace=True)

        # Replacing NaN an empty values in order to change content of the columns with IDs
        df.replace(pd.np.nan, 99999, inplace=True)
        df.replace(" ", 99999, inplace=True)

        # Changing columns with IDs trought cycle
        filling = ["has_job_or_business", "search_job_overseas", "search_job_mexico",
                    "search_start_business", "search_no_search", "search_no_knowledge",
                    "actual_frecuency_payments", "actual_minimal_wages_proportion", 
                    "actual_healthcare_attention", "second_activity"]

        # For cycle in order to change the content of a column from previous id, into the new ones (working for translate too)
        for sheet in filling:
            df_l = pd.read_excel(df_labels, sheet)
            df[sheet] = df[sheet].astype(int)
            df[sheet] = df[sheet].replace(dict(zip(df_l.prev_id, df_l.id)))

        # Turning back NaN values in the respective columns
        df.replace(99999, pd.np.nan, inplace=True)

        return df

class PopulationPipeline(EasyPipeline):
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
            "ent_id":   "UInt8",
            "time":     "UInt8"
        }

        download_step = DownloadStep(
            connector=["enoe-1-data", "enoe-2-data"],
            connector_path="conns.yaml"
        )
        transform_step = TransformStep()
        load_step = LoadStep(
            "inegi_enoe", db_connector, if_exists="append", pk=["ent_id", "time" ], dtype=dtype, 
            nullable_list=[
              "search_job_year", "actual_job_position", "actual_job_industry_group_id", "actual_job_hrs_worked_lastweek",
              "actual_amount_pesos", "second_activity_task", "second_activity_group_id"
            ]
        )

        return [download_step, transform_step, load_step]