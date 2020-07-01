import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import DownloadStep
from bamboo_lib.steps import LoadStep

from shared import STATE_REPLACE
from shared import slug_parser

class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        df = pd.read_csv(
            "http://www.conapo.gob.mx/work/models/CONAPO/Marginacion/Datos_Abiertos/Delimitacion_ZM/ZM_2015.csv",
            encoding="latin-1"
        )
        df = df[["CVE_ZM", "NOM_ZM", "CVE_MUN", "NOM_MUN", "CVE_ENT", "NOM_ENT"]]
        df = df.rename(columns={
            "CVE_ENT": "ent_id",
            "NOM_ENT": "ent_name",
            "CVE_ZM": "zm_id", 
            "NOM_ZM": "zm_name", 
            "CVE_MUN": "mun_id", 
            "NOM_MUN": "mun_name"
        })
        df["zm_id"] = ('99' + df["zm_id"].astype(str).str.replace(".", "")).astype(int)
        df["mun_id"] = df["mun_id"].astype(int)

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
        df["zm_slug"] = (df["zm_name"] + " zm mx").apply(slug_parser)

        df["ent_name"].replace(STATE_REPLACE, inplace=True)

        return df

class DimZMGeographyPipeline(EasyPipeline):
    @staticmethod
    def steps(params):
        db_connector = Connector.fetch('clickhouse-database', open("../conns.yaml"))

        dtype = {
            "ent_id":       "UInt8",
            "ent_name":     "String",
            "zm_id":        "UInt32",
            "zm_name":      "String",
            "mun_id":       "UInt16",
            "mun_name":     "String",
            "nation_name":  "String",
            "nation_id":    "String"
        }

        transform_step = TransformStep()
        load_step = LoadStep(
            "dim_shared_geography_zm", db_connector, if_exists="drop", dtype=dtype,
            pk=["zm_id", "mun_id"]
        )

        return [transform_step, load_step]

if __name__ == "__main__":
    pp = DimZMGeographyPipeline()
    pp.run({})