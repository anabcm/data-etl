import pandas as pd
from bamboo_lib.models import PipelineStep
from bamboo_lib.models import EasyPipeline
from bamboo_lib.connectors.models import Connector
from bamboo_lib.steps import LoadStep
# English stopwords
from sklearn.feature_extraction import stop_words

class ReadStep(PipelineStep):
    def run_step(self, prev, params):
        url = "https://docs.google.com/spreadsheets/d/e/2PACX-1vSZSlTqZjnTN5xaLBrlmG7djdbAUwDxsW4N7F6-4ljEi4ki1Q2QRWPzpBoeDYWyiPNVIeHCGOYxbOmD/pub?output=xlsx"
        df = pd.read_excel(url, sheet_name="academic_degree", encoding="latin-1")
        return df

class CleanStep(PipelineStep):
    def run_step(self, prev, params):
        df = prev

        # Setting column names to set in format
        stopwords_es = ["a", "ante", "con", "contra", "de", "desde", "la", "lo", "las", "los", "y"]

        # Step for spanish words
        df["academic_degree_es"] = df["academic_degree_es"].str.title()
        for ene in stopwords_es:
            df["academic_degree_es"] = df["academic_degree_es"].str.replace(" " + ene.title() + " ", " " + ene + " ")

        # Step for english words
        df["academic_degree_en"] = df["academic_degree_en"].str.title()
        for ene in list(stop_words.ENGLISH_STOP_WORDS):
            df["academic_degree_en"] = df["academic_degree_en"].str.replace(" " + ene.title() + " ", " " + ene + " ")

        # Groupby step
        grouped = ["id", "academic_degree_es", "academic_degree_en"]

        df = df.groupby(grouped).sum().reset_index(col_fill="ffill")

        return df

class CoveragePipeline(EasyPipeline):
    @staticmethod
    def description():
        return "Processes academic degrees from Mexico"

    @staticmethod
    def website():
        return "http://datawheel.us"

    @staticmethod
    def steps(params, **kwargs):
        # Use of connectors specified in the conns.yaml file
        db_connector = Connector.fetch("clickhouse-database", open("../conns.yaml"))
        dtype = {
            "id":                              "UInt8",
            "academic_degree_es":              "String",
            "academic_degree_en":              "String",
        }

        # Definition of each step
        read_step = ReadStep()
        clean_step = CleanStep()
        load_step = LoadStep("dim_shared_academic_degree", db_connector, if_exists="drop", pk=["id"], dtype=dtype)

        return [read_step, clean_step, load_step]