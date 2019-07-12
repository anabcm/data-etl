import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import DownloadStep
from bamboo_lib.steps import LoadStep


class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        df_labels = pd.ExcelFile("https://docs.google.com/spreadsheets/d/e/2PACX-1vSYxBmHW5xXkzhVL3X5N21EWWVJKzOkaCfEERaG5lWpmgdx6-Sjcxf7FA7uV1j-_EJIeWZmGbMMDeJh/pub?output=xlsx")

        df = pd.read_csv(prev, dtype=str, index_col=None, header=0, encoding="latin-1")
        df.columns = df.columns.str.lower()

        # Adding IDs columns and adding year column
        df["loc_id"] = df["ent"] + df["mun"] + df["loc50k"]
        df["mun_id_trab"] = df["ent_pais_trab"] + df["mun_trab"]
        df["year"] = 2015

        # Replacing NaN values with "X" (Not this df.fillna("0", inplace=True)) 
        # in order to not drop values by GroupBy method
        df["tie_traslado_trab"].fillna("0", inplace=True)
        df["med_traslado_trab1"].fillna("0", inplace=True)
        df["nivacad"].fillna("1000", inplace=True)
        df["conact"].fillna("0", inplace=True)
        df["mun_id_trab"].fillna("0", inplace=True)

        # Transforming certains str columns into int values
        df["loc_id"] = df["loc_id"].astype(int)
        df["mun_id_trab"] = df["mun_id_trab"].astype(int)
        df["factor"] = df["factor"].astype(int)
        df["edad"] = df["edad"].astype("int64")

        # Turning work places IDs to 0, which are overseas
        df.loc[df["mun_id_trab"] > 33000, "mun_id_trab"] = 0
        df["mun_id_trab"].replace(pd.np.nan , 0, inplace=True)

        # List of columns for the next df
        params = ["sexo", "parent", "sersalud", "dhsersal1", 
            "conact", "tie_traslado_trab", "med_traslado_trab1",
            "nivacad"]

        params_translated = ["sex", "parent", "sersalud", "dhsersal1",
            "laboral_condition", "time_to_work", "transport_mean_work",
            "academic_degree"]

        # For cycle in order to change the content of a column from previous id, into the new ones (working for translate too)
        for sheet in params:
            df_l = pd.read_excel(df_labels, sheet)
            df[sheet] = df[sheet].astype(int)
            df[sheet] = df[sheet].replace(dict(zip(df_l.prev_id, df_l.id)))

        # Renaming of certains columns (Nacionality is not added given 2010 data, for now)
        df.rename(index=str, columns={
                            "factor": "population",
                            "sexo": "sex", 
                            "conact": "laboral_condition", 
                            "tie_traslado_trab": "time_to_work",
                            "med_traslado_trab1": "transport_mean_work",
                            "nivacad": "academic_degree"}, inplace=True)


        url = "https://docs.google.com/spreadsheets/d/e/2PACX-1vSYxBmHW5xXkzhVL3X5N21EWWVJKzOkaCfEERaG5lWpmgdx6-Sjcxf7FA7uV1j-_EJIeWZmGbMMDeJh/pub?output=xlsx"
        pivote = pd.read_excel(url, sheet_name="tramo_edad", encoding="latin-1", dtype={"interval_upper": "int64", "interval_lower": "int64"})
        for edad in df.age.unique():
            for level in range(pivote.shape[0]):
                if (edad >= pivote.interval_lower[level]) & (edad < pivote.interval_upper[level]):
                    df.age.replace(edad, str(pivote.id[level]), inplace=True)
                    break
        df.age = df.age.astype("int")

        # Condense df around params list, mun_id and loc_id, and sum over population (factor)
        df = df.groupby(params_translated + ["loc_id", "mun_id_trab", "age"]).sum().reset_index(col_fill="ffill")

        # Turning back NaN values in the respective columns
        df["time_to_work"].replace(0, pd.np.nan, inplace=True)
        df["transport_mean_work"].replace(0, pd.np.nan, inplace=True)
        df["academic_degree"].replace(1000, pd.np.nan, inplace=True)
        df["laboral_condition"].replace(0, pd.np.nan, inplace=True)
        df["mun_id_trab"].replace(0, pd.np.nan, inplace=True)

        # Not answered age values, turned to text (Column as object type)
        df["age"].replace(999, "Edad no especificada", inplace=True)

        # Transforming certains columns to objects
        for col in (params_translated + ["mun_id_trab"]):
            df[col] = df[col].astype("object")

        return df

class PopulationPipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return [
            Parameter(label="Index", name="index", dtype=str)
        ]

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch("clickhouse-database", open("../conns.yaml"))

        dtype = {
            "sex":                 "UInt8",
            "loc_id":              "UInt32",
            "population":          "UInt64",
            "parent":              "UInt8",
            "sersalud":            "UInt8",
            "dhsersal1":           "UInt8",
            "laboral_condition":   "UInt8",
            "time_to_work":        "UInt8",
            "transport_mean_work": "UInt8",
            "mun_id_trab":         "UInt8",
            "academic_degree":     "UInt8",
            "age":                 "UInt8",
            "year":                "UInt8",
        }

        download_step = DownloadStep(
            connector="population-data",
            connector_path="conns.yaml"
        )
        transform_step = TransformStep()
        load_step = LoadStep(
            "inegi_population", db_connector, if_exists="append", pk=["loc_id", "sex"], dtype=dtype, 
            nullable_list=[
                "time_to_work", "transport_mean_work", "laboral_condition", "mun_id_trab"]
        )

        return [download_step, transform_step, load_step]
