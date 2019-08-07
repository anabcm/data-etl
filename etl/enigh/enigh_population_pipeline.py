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
        list_cols = ["folioviv", "foliohog", "sexo", "edad", "hablaind", "hablaesp", "etnia", "nivelaprob", "residencia", "segsoc", "ss_aa", "ss_mm",
        "segsoc", "redsoc_1", "redsoc_2", "redsoc_3", "redsoc_4", "redsoc_5", "redsoc_6", "hor_1", "usotiempo1", "segpop",
        "atemed", "inst_1", "inst_2", "inst_3", "inst_4", "inst_5", "inst_6", "hh_lug", "mm_lug", "hh_esp", "mm_esp",
        "trabajo_mp", "motivo_aus", "act_pnea1", "act_pnea2", "num_trabaj"]

        dt_1 = pd.read_csv(prev[0], index_col=None, header=0, encoding="latin-1", dtype=str, usecols = list_cols)

        dt_1["usotiempo1"].replace({"8": "No recuerda", "9": "No lo hizo"}, inplace=True)

        dt_1.loc[(dt_1["hor_1"] != " "), "usotiempo1"] = dt_1["hor_1"]

        dt_1["ss_aa"].replace("-1", "No especificado", inplace = True)
        dt_1["ss_mm"].replace("-1", "No especificado", inplace = True)

        dt_1["near_healthcare_center"] = dt_1["hh_lug"] + ":" + dt_1["mm_lug"]
        dt_1["waiting_health_attention"] = dt_1["hh_esp"] + ":" + dt_1["mm_esp"]

        dt_1.replace(" ", 999999, inplace = True)

        df_viv = pd.read_csv(prev[1], index_col=None, header=0, encoding="latin-1", dtype=str,
                usecols= ["folioviv", "ubica_geo", "est_socio", "factor", "tam_loc"])


        df = pd.merge(dt_1,
                      df_viv[["folioviv", "ubica_geo", "est_socio", "factor", "tam_loc"]],
                      on="folioviv",
                      how="left")

        df["mun_id"] = df["ubica_geo"].str.slice(0,5)


        # Changing columns with IDs trought cycle
        filling = ["sexo", "hablaind", "hablaesp", "etnia", "nivelaprob", "residencia", "segsoc", "redsoc_1",
                    "redsoc_2", "redsoc_3", "redsoc_4", "redsoc_5", "redsoc_6", "segpop",
                    "atemed", "inst_1", "inst_2", "inst_3", "inst_4", "inst_5", "inst_6",
                    "trabajo_mp", "motivo_aus", "act_pnea1", "act_pnea2", "num_trabaj", "est_socio", "tam_loc"]

        # For cycle in order to change the content of a column from previous id, into the new ones (working for translate too)
        for sheet in filling:
            df_l = pd.read_excel(df_labels, sheet)
            df[sheet] = df[sheet].astype(float)
            df[sheet] = df[sheet].replace(dict(zip(df_l.prev_id, df_l.id)))

        # Turning back NaN values in the respective columns
        df.replace(999999, pd.np.nan, inplace = True)

        # Changing types for certains columns
        for col in []:
            df[col] = df[col].astype(float)

        for item in []:
            df[item] = df[item].astype(int)

        return df

class ENOEPipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return [
            Parameter(label="Year", name="year", dtype=str),
        ]

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch("clickhouse-database", open("../conns.yaml"))

        dtype = {
        }

        download_step = DownloadStep(
            connector=["", ""],
            connector_path="conns.yaml"
        )
        transform_step = TransformStep()
        load_step = LoadStep(
            "inegi_enigh_population", db_connector, if_exists="append", pk=["mun_id", "sex"], dtype=dtype, 
            nullable_list=[]
        )

        return [download_step, transform_step, load_step]