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
        df_labels = "https://docs.google.com/spreadsheets/d/e/2PACX-1vQLU-DPkD07hFCX1xSavFZUgXDZvOQclDplsRvE14_hOR6XZmXAyiiii5Q3CEI_w59a58aElemloSO_/pub?output=xlsx"

        # Loading population file
        dt_1 = pd.read_csv(prev[0], index_col=None, header=0, encoding="latin-1", dtype=str,
                                    usecols = ["folioviv", "foliohog","numren", "id_trabajo", "trapais",
                                              "pago", "contrato", "tipocontr", "htrab", "sinco", "scian",
                                              "clas_emp", "tam_emp"])

        # Replacing empty cells in columns with actual numbers
        dt_1["pago"].replace(" ", "99", inplace = True)
        dt_1["clas_emp"].replace(" ", "99", inplace = True)

        # Transforming the numbers to int type values
        dt_1["pago"] = dt_1["pago"].astype(int)
        dt_1["clas_emp"] = dt_1["clas_emp"].astype(int)

        # Loading enigh housing dataframe in order to get mun_id and factor columns
        df_viv = pd.read_csv(prev[1], index_col=None, header=0, encoding="latin-1", dtype=str,
                usecols=["folioviv", "ubica_geo", "est_socio", "factor", "tam_loc"])

        # Merging housing with jobs dataframe
        df = pd.merge(dt_1, df_viv[["folioviv", "ubica_geo", "est_socio", "factor", "tam_loc"]],
                    on="folioviv", how="left")
        df["mun_id"] = df["ubica_geo"].str.slice(0,5)

        # Loading enigh population dataframe in order to get sex and age columns
        df_pop = pd.read_csv(prev[2], index_col=None, header=0, encoding="latin-1", dtype=str,
                usecols=["folioviv", "foliohog", "numren", "sexo", "edad"])

        # Common column in order to merge population dataframe
        df["coding"] = df["folioviv"] + df["foliohog"] + df["numren"]
        df_pop["coding"] = df_pop["folioviv"] + df_pop["foliohog"] + df_pop["numren"]

        df = pd.merge(df, df_pop[["coding", "sexo"]], on="coding", how="left")

        # Changing columns with IDs trought cycle
        filling = ["id_trabajo", "pago", "clas_emp", "tam_emp", "sexo", "est_socio", "tam_loc"]

        # For cycle in order to change the content of a column from previous id, into the new ones (working for translate too)
        for sheet in filling:
            df_l = pd.read_excel(df_labels, sheet)
            df[sheet] = df[sheet].astype(float)
            df[sheet] = df[sheet].replace(dict(zip(df_l.prev_id, df_l.id)))

        # Returning odd values to empty cells, to match next step
            df["pago"].replace(99, " ", inplace = True)
            df["clas_emp"].replace(99, " ", inplace = True)

        # Turning " " to NaN values in the dataframe
        df.replace(" ",pd.np.nan, inplace = True)

        # Droping already used columns
        list_drop = ["coding", "ubica_geo"]
        df.drop(list_drop, axis=1, inplace=True)

        # Renaming the columns to english
        params = {
            "sexo": "sex", 
            "edad": "age",
            "id_trabajo": "id_job",
            "trapais": "national_job",
            "htrab": "worked_hours",
            "sinco": "sinco_id",
            "scian": "scian_id",
            "est_socio": "eco_stratum",
            "tam_loc": "loc_size",
            "factor": "population",
            "pago": "pay_mode",
            "contrato": "contract",
            "tipocontr": "contract_type",
            "tam_emp": "business_size",
            "clas_emp": "business_type"
            }
        df.rename(index=str, columns=params, inplace=True)

        # Changing types for certains columns
        non_null_list = ["sex", "age", "id_job", "national_job", "worked_hours", 
                        "sinco_id", "scian_id", "eco_stratum", "loc_size", "population",
                        "business_size", "mun_id", "folioviv", "foliohog", "numren"]

        for col in ["pay_mode", "contract", "contract_type", "business_type"]:
            df[col] = df[col].astype(float)

        for item in non_null_list:
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
            "numren"                          "UInt8",
            "hablaesp"                        "UInt8",
            "etnicity"                        "UInt8",
            "id_job"                          "UInt8",
            "national_job"                    "UInt8",
            "pay_mode"                        "UInt8",
            "contract"                        "UInt8",
            "contract_type"                   "UInt8",
            "worked_hours"                    "UInt8",
            "sinco_id"                        "UInt16",
            "scian_id"                        "UInt16",
            "business_type"                   "UInt8",
            "business_size"                   "UInt8",
            "eco_stratum"                     "UInt8",
            "mun_id"                          "UInt16",
            "loc_size"                        "UInt8",
            "population"                      "UInt16",
            "eco_stratum"                     "UInt8",
            "folioviv"                        "UInt32",
            "foliohog"                        "UInt8",
            "sex"                             "UInt8",
            "age"                             "UInt8",
        }

        download_step = DownloadStep(
            connector=["enigh-job", "enigh-housing", "enigh-population"],
            connector_path="conns.yaml"
        )
        transform_step = TransformStep()
        load_step = LoadStep(
            "inegi_enigh_population", db_connector, if_exists="append", pk=["mun_id", "sex"], dtype=dtype, 
            nullable_list=[]
        )

        return [download_step, transform_step, load_step]