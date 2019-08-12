import pandas as pd
import unidecode
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import DownloadStep
from bamboo_lib.steps import LoadStep

class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        df = pd.read_csv(
            "http://www.conapo.gob.mx/work/models/CONAPO/Marginacion/Datos_Abiertos/SUN/Base_SUN_2018.csv",
            encoding="latin-1"
        )
        df = df[["CVE_ENT", "NOM_ENT", "CVE_MUN", "NOM_MUN", "CVE_LOC", "NOM_LOC", "CVE_SUN", "NOM_SUN"]]
        df = df.rename(columns={
            "CVE_ENT": "ent_id",
            "NOM_ENT": "ent_name",
            "CVE_MUN": "mun_id",
            "NOM_MUN": "mun_name",
            "CVE_LOC": "loc_id",
            "NOM_LOC": "loc_name",
            "CVE_SUN": "sun_id",
            "NOM_SUN": "sun_name"
        })
        df["sun_id"] = df["sun_id"].str.replace(".", "")
        df["ent_id"] = df["ent_id"].astype(int)
        df["mun_id"] = df["mun_id"].astype(int)
        df["loc_id"] = df["loc_id"].astype(object)

        df["nation_id"] = "mex"
        df["nation_name"] = "México"
        df["nation_slug"] = "mexico"

        ent_iso2 = {
            1: "AG", 2: "BC", 3: "BS", 4: "CM", 5: "CS", 6: "CH", 7: "CX", 8: "CO",
            9: "CL", 10: "DG", 11: "GT", 12: "GR", 13: "HG", 14: "JC", 15: "EM", 16: "MI",
            17: "MO", 18: "NA", 19: "NL", 20: "OA", 21: "PU", 22: "QT", 23: "QR", 24: "SL",
            25: "SI", 26: "SO", 27: "TB", 28: "TM", 29: "TL", 30: "VE", 31: "YU", 32: "ZA"
        }

        df["ent_iso2"] = df["ent_id"].replace(ent_iso2)

        df["ent_slug"] = (df["ent_name"] + " " + df["ent_iso2"]).apply(slug_parser)
        df["sun_slug"] = (df["sun_name"] + " sun mx").apply(slug_parser)

        return df

class DimSUNGeographyPipeline(EasyPipeline):
    @staticmethod
    def steps(params):
        db_connector = Connector.fetch('clickhouse-database', open("../conns.yaml"))

        dtype = {
            "ent_id":       "UInt8",
            "ent_name":     "String",
            "sun_id":       "String",
            "sun_name":     "String",
            "mun_id":       "UInt16",
            "mun_name":     "String",
            "loc_id":       "UInt32",
            "loc_name":     "String",
            "nation_name":  "String",
            "nation_id":    "String"
        }

        transform_step = TransformStep()
        load_step = LoadStep(
            "dim_shared_geography_sun", db_connector, if_exists="drop", dtype=dtype,
            pk=["sun_id", "mun_id"], nullable_list=["loc_id", "loc_name"]
        )

        return [transform_step, load_step]
