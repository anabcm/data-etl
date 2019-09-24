import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import DownloadStep
from bamboo_lib.steps import LoadStep
from simpledbf import Dbf5 #required given data files

class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        # list 1 and 2 given different headers names 
        list_1 = ["CD", "ENT", "MUN", "LOC", "BP1_1", "BP1_2_01", "BP1_2_02", "BP1_2_03", "BP1_2_09", "BP1_3", "BP1_4_3", 
                  "BP1_4_4", "BP1_4_5", "BP1_4_6", "BP1_5_1", "BP1_5_2", "BP1_7_1", "BP1_7_2", "BP1_7_3", "BP1_7_4", "BP1_8_1",
                  "BP1_8_2", "BP1_8_3", "BP1_8_4", "BP2_2_10", "BP2_2_14", "BP2_4_1", "BP2_4_4", "BP2_4_5", "BP2_4_6", "BP2_4_7", 
                  "BP3_2", "FAC_SEL"]

        list_2 = ["CD", "CVE_ENT", "CVE_MUN", "LOC", "BP1_1", "BP1_2_01", "BP1_2_02", "BP1_2_03", "BP1_2_09", "BP1_3", "BP1_4_3",
                  "BP1_4_4", "BP1_4_5", "BP1_4_6", "BP1_5_1", "BP1_5_2", "BP1_7_1", "BP1_7_2", "BP1_7_3", "BP1_7_4", "BP1_8_1",
                  "BP1_8_2", "BP1_8_3", "BP1_8_4", "BP2_2_10", "BP2_2_14", "BP2_4_1", "BP2_4_4", "BP2_4_5", "BP2_4_6", "BP2_4_7", 
                  "BP3_2", "FAC_SEL"]

        # Read step given the 2 headers list
        try:
            dbf = Dbf5(prev, codec="latin-1")
            df = dbf.to_dataframe()
            df = df[list_1]
        except:
            dbf = Dbf5(prev, codec="latin-1")
            df = dbf.to_dataframe()
            df = df[list_2]

        # Rename to match the mayority of files
        df.rename(index=str, columns={"ENT": "CVE_ENT", "MUN": "CVE_MUN"}, inplace=True)

        # creating loc_id column and droping used columns
        df["loc_id"] = df["CVE_ENT"] + df["CVE_MUN"] + df["LOC"]
        df.drop(["CVE_ENT", "CVE_MUN", "LOC"], axis=1, inplace=True)

        # Getting values of year and respective quarter for the survey
        df["quarter_id"] = "20" + params["year"] + params["quarter"]
        df["quarter_id"] = df["quarter_id"].astype(int)

        # Rename list
        coding = {
            "CD": "reference_city", "BP1_1": "perception_city", "BP1_2_01": "perception_city_home",
            "BP1_2_02": "perception_city_at_work", "BP1_2_03": "perception_city_streets", "BP1_2_09": "perception_city_transport",
            "BP1_3": "city_improves", "BP1_4_3": "thefts_3_months", "BP1_4_4": "gangs_3_months", "BP1_4_5": "drugs_3_months",
            "BP1_4_6": "gunshots_3_months", "BP1_5_1":"carrying_goods", "BP1_5_2":"walks_night", "BP1_7_1": "preventive_police",
            "BP1_7_2": "state_police", "BP1_7_3": "federal_police", "BP1_7_4": "gendarmerie_police", "BP2_2_10": "fights_gangs_3_months",
            "BP2_2_14":"police_abuse_3_months", "BP2_4_1": "conflict_screams","BP2_4_4": "conflict_hits", "BP2_4_5": "conflict_with_obj",
            "BP2_4_6": "conflict_sharp_obj", "BP2_4_7": "conflict_guns", "BP3_2": "goverment_actions", "FAC_SEL": "population",
            "BP1_8_1": "preventive_police_perception", "BP1_8_2": "state_police_perception", "BP1_8_3": "federal_police_perception",
            "BP1_8_4": "gendarmerie_police_perception" 
        }

        # Renaming step
        df.rename(columns=dict(coding), inplace=True)

        # Replacing nan values in order to pass the groupby method
        df.replace(pd.np.nan, "99", inplace = True)
        df["population"] = df["population"].astype(int)

        # Groupby method
        grouped = [
            "reference_city", "perception_city", "perception_city_home", "perception_city_at_work",
            "perception_city_streets",
            "perception_city_transport", "city_improves", "thefts_3_months", "gangs_3_months",
            "drugs_3_months", "gunshots_3_months", "carrying_goods",
            "walks_night", "preventive_police", "state_police", "federal_police",
            "gendarmerie_police", "fights_gangs_3_months", "police_abuse_3_months",
            "conflict_screams","conflict_hits",
            "conflict_with_obj", "conflict_sharp_obj", "conflict_guns",
            "goverment_actions", "preventive_police_perception",
            "state_police_perception", "federal_police_perception", "gendarmerie_police_perception", "loc_id", "quarter_id"
        ]

        df = df.groupby(grouped).sum().reset_index(col_fill="ffill")

        # Turning back nan values
        df.replace(pd.np.nan, "99", inplace = True)

        # Changing str values to float/int values
        for col in grouped:
            df[col] = df[col].astype(float)

        return df

class EnsuPipeline(EasyPipeline):
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
              "reference_city":                     "UInt8",
              "perception_city":                    "UInt8",
              "perception_city_home":               "UInt8",
              "perception_city_at_work":            "UInt8",
              "perception_city_streets":            "UInt8",
              "perception_city_transport":          "UInt8",
              "city_improves":                      "UInt8",
              "thefts_3_months":                    "UInt8",
              "gangs_3_months":                     "UInt8",
              "drugs_3_months":                     "UInt8",
              "gunshots_3_months":                  "UInt8",
              "carrying_goods":                     "UInt8",
              "walks_night":                        "UInt8",
              "preventive_police":                  "UInt8",
              "state_police":                       "UInt8",
              "federal_police":                     "UInt8",
              "gendarmerie_police":                 "UInt8",
              "fights_gangs_3_months":              "UInt8",
              "police_abuse_3_months":              "UInt8",
              "conflict_screams":                   "UInt8",
              "conflict_hits":                      "UInt8",
              "conflict_with_obj":                  "UInt8",
              "conflict_sharp_obj":                 "UInt8",
              "conflict_guns":                      "UInt8",
              "goverment_actions":                  "UInt8",
              "preventive_police_perception":       "UInt8",
              "state_police_perception":            "UInt8",
              "federal_police_perception":          "UInt8",
              "gendarmerie_police_perception":      "UInt8",
              "loc_id":                             "UInt32",
              "quarter_id":                         "UInt16",
              "population":                         "UInt16"
        }

        download_step = DownloadStep(
            connector="ensu-data",
            connector_path="conns.yaml"
        )
        transform_step = TransformStep()
        load_step = LoadStep(
            "inegi_ensu", db_connector, if_exists="append",
            pk=["reference_city", "quarter_id", "loc_id"], dtype=dtype,
            nullable_list=["preventive_police", "state_police", "federal_police", "gendarmerie_police", "preventive_police_perception",
                "state_police_perception", "federal_police_perception", "gendarmerie_police_perception", "fights_gangs_3_months",
                "police_abuse_3_months", "conflict_screams", "conflict_hits", "conflict_with_obj", "conflict_sharp_obj",
                "conflict_guns", "goverment_actions"]
        )

        return [download_step, transform_step, load_step]