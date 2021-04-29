import numpy as np
import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import DownloadStep
from bamboo_lib.steps import LoadStep
from util import extra_columns


class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        df_labels = pd.ExcelFile(prev[1])

        df = pd.read_csv(prev[0], dtype=str, index_col=None, header=0, encoding="latin-1")
        df.columns = df.columns.str.lower()
        
        extra_columns_low = [col.lower() for col in extra_columns]

        for column in extra_columns_low:
            if column not in df.columns:
                df[column] = np.nan
        
        if "parentesco" in df.columns:
            df.rename(columns={'parentesco':'parent'}, inplace=True)

        # Adding IDs columns
        df["mun_id"] = df["ent"].astype(str).str.zfill(2) + df["mun"].astype(str).str.zfill(3)
        df["mun_id_trab"] = df["ent_pais_trab"] + df["mun_trab"]

        # Replacing NaN values with "X" (Not this df.fillna("0", inplace=True)) 
        # in order to not drop values by GroupBy method
        li_spa = ["tie_traslado_trab", "med_traslado_trab1", "tie_traslado_escu",
                    "med_traslado_esc1", "conact", "mun_id_trab", "nacionalidad", "qdialect_inali", "alfabet", "asisten"]

        li_eng = ["time_to_work", "transport_mean_work", "time_to_ed_facilities",
                    "transport_mean_ed_facilities", "laboral_condition", "mun_id_trab", "nationality", "indigenous_language_id", "literate", "school_attendance"]

        for item in li_spa + extra_columns_low:
            df[item].fillna("0", inplace=True)

        df["nivacad"].fillna("1000", inplace=True)
        df["nivacad"] = df["nivacad"].str.zfill(2)
        
        # Transforming certains str columns into int values
        df["mun_id"] = df["mun_id"].astype(int)
        df["mun_id_trab"] = df["mun_id_trab"].astype(int)
        df["factor"] = df["factor"].astype(int)
        df["edad"] = df["edad"].astype(int)

        # Turning work places IDs to 0, which are overseas
        df.loc[df["mun_id_trab"] > 33000, "mun_id_trab"] = 0
        df["mun_id_trab"].replace(np.nan , 0, inplace=True)

        # List of columns for the next df
        parameters = ["sexo", "parent", "sersalud", "dhsersal1", 
        "conact", "tie_traslado_trab", "med_traslado_trab1",
        "nivacad", "tie_traslado_escu", "med_traslado_esc1",
        "nacionalidad"]

        parameters_add1 = ["qdialect_inali", "religion", "alfabet", 
        "ent_pais_nac", "ent_pais_res_5a", "asisten", "causa_mig"]

        params_translated = ["sex", "parent", "sersalud", "dhsersal1",
        "laboral_condition", "time_to_work", "transport_mean_work",
        "academic_degree", "time_to_ed_facilities", "transport_mean_ed_facilities",
        "nationality"]
        
        params_translated_add1 = ["location_key", "state_of_birth", "afro_offspring", "religion",
        "indigenous_speaker", "indigenous_language_id", "indigenous_self_ascribing", "visual_impairment", "hearing_imperiment",
        "walk_imperiment", "remembering_imperiment", "washing_imperiment", "talk_imperiment",
        "mental_imperiment", "visual_imp_cause", "hearing_imp_cause", "walk_imp_cause",
        "remembering_imp_cause", "washing_imp_cause", "talk_imp_cause", "mental_imp_cause",
        "school_attendance", "literate", "state_of_residency", "municipality_of_residency",
        "migration_cause", "children_born_alive", "deceased_children", "location_size"]

        # For cycle in order to change the content of a column from previous id, into the new ones (working for translate too)
        for sheet in parameters + parameters_add1:
            df_l = pd.read_excel(df_labels, sheet)
            df[sheet] = df[sheet].astype(int)
            df[sheet] = df[sheet].replace(dict(zip(df_l.prev_id, df_l.id)))

        # Renaming of certains columns (Nacionality is not added given 2010 data, for now)
        df.rename(index=str, columns={
                            "loc50k": "location_key",
                            "factor": "population",
                            "sexo": "sex",
                            "edad": "age",
                            "ent_pais_nac": "state_of_birth",
                            "afrodes": "afro_offspring",
                            "conact": "laboral_condition",
                            "tie_traslado_trab": "time_to_work",
                            "med_traslado_trab1": "transport_mean_work",
                            "nivacad": "academic_degree",
                            "tie_traslado_escu": "time_to_ed_facilities",
                            "med_traslado_esc1": "transport_mean_ed_facilities",
                            "nacionalidad": "nationality",
                            "dis_ver": "visual_impairment",
                            "dis_oir": "hearing_imperiment",
                            "dis_caminar": "walk_imperiment",
                            "dis_recordar": "remembering_imperiment",
                            "dis_banarse": "washing_imperiment",
                            "dis_hablar": "talk_imperiment",
                            "dis_mental": "mental_imperiment",
                            "cau_ver": "visual_imp_cause",
                            "cau_oir": "hearing_imp_cause",
                            "cau_caminar": "walk_imp_cause",
                            "cau_recordar": "remembering_imp_cause",
                            "cau_banarse": "washing_imp_cause",
                            "cau_hablar": "talk_imp_cause",
                            "cau_mental": "mental_imp_cause",
                            "hlengua": "indigenous_speaker",
                            "qdialect_inali": "indigenous_language_id",
                            "perte_indigena": "indigenous_self_ascribing",
                            "asisten": "school_attendance",
                            "alfabet": "literate",
                            "ent_pais_res_5a": "state_of_residency",
                            "mun_res_5a": "municipality_of_residency",
                            "causa_mig": "migration_cause",
                            "hijos_nac_vivos": "children_born_alive",
                            "hijos_fallecidos": "deceased_children",
                            "tamloc": "location_size"
                            }, inplace=True)

        # Condense df around params list, mun_id, and sum over population (factor)

        # df.fillna(888888, inplace=True)
        for col in params_translated_add1:
            df[col].fillna(888888, inplace=True)
            df[col] = df[col].astype(int)
  
        df = df.groupby(params_translated + params_translated_add1 + ["mun_id", "mun_id_trab", "age"]).sum().reset_index(col_fill="ffill")

        # Turning back NaN values in the respective columns
        for item in li_eng:
            df[item].replace(0, np.nan, inplace=True)
        df["academic_degree"].replace(1000, np.nan, inplace=True)
        df["age"].replace(999, np.nan, inplace=True)
        df["filtered_age"] = df['age'].apply(lambda x: 1 if x >= 12 else 0)

        for column in params_translated_add1:
            df[column] = df[column].apply(lambda x : x if x != 888888 else np.nan)
         
        # Includes year column
        df["year"] = params.get("year")

        # Transforming certains columns to objects
        for col in (params_translated + params_translated_add1 + ["mun_id_trab"]):
            df[col] = df[col].astype("object")
      
        return df

class PopulationPipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return [
            Parameter(label="Index", name="index", dtype=str),
            Parameter(label="Source", name="source", dtype=str),
            Parameter(label="Year", name="year", dtype=str)
        ]

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch("clickhouse-database", open("../conns.yaml"))

        dtype = {
            "sex":                          "UInt8",
            "mun_id":                       "UInt16",
            "population":                   "UInt64",
            "parent":                       "UInt8",
            "nationality":                  "UInt8",
            "sersalud":                     "UInt8",
            "dhsersal1":                    "UInt8",
            "laboral_condition":            "UInt8",
            "time_to_work":                 "UInt8",
            "transport_mean_work":          "UInt8",
            "time_to_ed_facilities":        "UInt8",
            "transport_mean_ed_facilities": "UInt8",
            "mun_id_trab":                  "UInt8",
            "academic_degree":              "UInt8",
            "age":                          "UInt8",
            "filtered_age":                 "UInt8",
            "year":                         "UInt16",
            "location_key":                 "UInt16", 
            "state_of_birth":               "UInt16", 
            "afro_offspring":               "UInt8", 
            "religion":                     "UInt8",
            "indigenous_speaker":           "UInt8", 
            "indigenous_language_id":       "UInt8", 
            "indigenous_self_ascribing":    "UInt8", 
            "visual_impairment":            "UInt8",
            "hearing_imperiment":           "UInt8",
            "walk_imperiment":              "UInt8", 
            "remembering_imperiment":       "UInt8", 
            "washing_imperiment":           "UInt8", 
            "talk_imperiment":              "UInt8",
            "mental_imperiment":            "UInt8", 
            "visual_imp_cause":             "UInt8", 
            "hearing_imp_cause":            "UInt8", 
            "walk_imp_cause":               "UInt8",
            "remembering_imp_cause":        "UInt8", 
            "washing_imp_cause":            "UInt8", 
            "talk_imp_cause":               "UInt8", 
            "mental_imp_cause":             "UInt8",
            "school_attendance":            "UInt8", 
            "literate":                     "UInt8", 
            "state_of_residency":           "UInt16", 
            "municipality_of_residency":    "UInt16",
            "migration_cause":              "UInt8", 
            "children_born_alive":          "UInt8", 
            "deceased_children":            "UInt8", 
            "location_size":                "UInt16"
        }

        download_step = DownloadStep(
            connector=[params.get("source"), "labels-2"],
            connector_path="conns.yaml"
        )
        transform_step = TransformStep()
        load_step = LoadStep(
            "inegi_population", db_connector, if_exists="append", pk=["mun_id", "sex"], dtype=dtype, 
            nullable_list=["age", "time_to_work", "transport_mean_work", "school_attendance", "literate", "children_born_alive",
            "time_to_ed_facilities", "transport_mean_ed_facilities", "laboral_condition", "indigenous_self_ascribing", "deceased_children",
            "mun_id_trab", "academic_degree", "nationality", "indigenous_language_id", "indigenous_speaker"]
        )

        return [download_step, transform_step, load_step]

