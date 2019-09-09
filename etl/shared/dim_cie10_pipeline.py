import pandas as pd
from bamboo_lib.models import PipelineStep
from bamboo_lib.models import EasyPipeline
from bamboo_lib.connectors.models import Connector
from bamboo_lib.steps import LoadStep
# English stopwords
from sklearn.feature_extraction import stop_words

class ReadStep(PipelineStep):
    def run_step(self, prev, params):
        url = "https://docs.google.com/spreadsheets/d/e/2PACX-1vSBBIm3kROrey05M9VoRDvGM7uWIQis-Xx1Zy4x1zVvuGPv39JFAcW6Vi7kRW2PgInjRgNecEnxXi5T/pub?output=xlsx"
        df = pd.read_excel(url, dtype="str")
        return df

class CleanStep(PipelineStep):
    def run_step(self, prev, params):
        df = prev
								
        # Setting column names to set in format
        cols_es = ["category_es", "cie10_3digit_es", "cie10_4digit_es"]
        cols_en = ["category_en", "cie10_3digit_en", "cie10_4digit_en"]
        stopwords_es = ["a", "ante", "con", "contra", "de", "desde", "la", "lo", "las", "los", "y"]

        # Step for spanish words
        for ele in cols_es:
            df[ele] = df[ele].str.title()
            for ene in stopwords_es:
                df[ele] = df[ele].str.replace(" " + ene.title() + " ", " " + ene + " ")

        # Step for english words
        for ele in cols_en:
            df[ele] = df[ele].str.title()
            for ene in list(stop_words.ENGLISH_STOP_WORDS):
                df[ele] = df[ele].str.replace(" " + ene.title() + " ", " " + ene + " ")

        # Groupby step
        grouped = ["chapter_id", "category_es", "category_en", "cie10_3digit", "cie10_3digit_es", "cie10_3digit_en",
           "cie10_4digit", "cie10_4digit_es", "cie10_4digit_en"]

        df = df.groupby(grouped).sum().reset_index(col_fill="ffill")

        return df

class CoveragePipeline(EasyPipeline):
    @staticmethod
    def description():
        return "Processes IDC codes from Mexico"

    @staticmethod
    def website():
        return "http://datawheel.us"

    @staticmethod
    def steps(params, **kwargs):
        # Use of connectors specified in the conns.yaml file
        db_connector = Connector.fetch("clickhouse-database", open("../conns.yaml"))
        dtype = {
            "chapter_id":                "String",
            "category_es":              "String",
            "category_en":              "String",
            "cie10_3digit":             "String",
            "cie10_3digit_es":          "String",
            "cie10_3digit_en":          "String",
            "cie10_4digit":             "String",
            "cie10_4digit_es":          "String",
            "cie10_4digit_en":          "String"
        }

        # Definition of each step
        read_step = ReadStep()
        clean_step = CleanStep()
        load_step = LoadStep(
            "dim_shared_cie10", db_connector, if_exists="drop", pk=["chapter_id", "cie10_3digit", "cie10_4digit"], dtype=dtype
        )
        
        return [read_step, clean_step, load_step]