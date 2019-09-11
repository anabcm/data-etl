import pandas as pd
from bamboo_lib.models import PipelineStep
from bamboo_lib.models import EasyPipeline
from bamboo_lib.connectors.models import Connector
from bamboo_lib.steps import LoadStep
# English stopwords
from sklearn.feature_extraction import stop_words

class ReadStep(PipelineStep):
    def run_step(self, prev, params):
        url = "https://docs.google.com/spreadsheets/d/e/2PACX-1vR6a5TEhnjteU3aa96H7iG4OoNAMkCgsQJ52HPbycoStRBIer66JnfsSbS5tbmlkdQ6jwn2dp8xBb0U/pub?output=xlsx"
        df = pd.read_excel(url, "crime_subtype", dtype="str")
        return df

class CleanStep(PipelineStep):
    def run_step(self, prev, params):
        df = prev
        # Setting column names to set in format
        cols_es = ["affected_legal_good_es", "crime_type_es", "crime_subtype_es"]
        cols_en = ["affected_legal_good_en", "crime_type_en", "crime_subtype_en"]
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
        grouped = ["affected_legal_good_id", "crime_type_id", "crime_subtype_id",
                   "affected_legal_good_es", "crime_type_es", "crime_subtype_es",
                   "affected_legal_good_en", "crime_type_en", "crime_subtype_en"]

        df = df.groupby(grouped).sum().reset_index(col_fill="ffill")

        return df

class CrimesSubtypePipeline(EasyPipeline):
    @staticmethod
    def description():
        return "Processes crimes codification to Mexico Crimes Data"

    @staticmethod
    def website():
        return "http://datawheel.us"

    @staticmethod
    def steps(params, **kwargs):
        # Use of connectors specified in the conns.yaml file
        db_connector = Connector.fetch("clickhouse-database", open("../conns.yaml"))
        dtype = {
            "affected_legal_good_id":                "UInt8",
            "crime_type_id":                         "UInt16",
            "crime_subtype_id":                      "UInt32",
            "affected_legal_good_es":                "String",
            "crime_type_es":                         "String",
            "crime_subtype_es":                      "String",
            "affected_legal_good_en":                "String",
            "crime_type_en":                         "String",
            "crime_subtype_en":                      "String"
        }

        # Definition of each step
        read_step = ReadStep()
        clean_step = CleanStep()
        load_step = LoadStep(
            "dim_shared_crimes_subtype", db_connector, if_exists="drop", pk=["affected_legal_good_id", "crime_type_id", "crime_subtype_id"], dtype=dtype
        )
        
        return [read_step, clean_step, load_step]