import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import DownloadStep
from bamboo_lib.steps import LoadStep

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
        df["zm_id"] = df["zm_id"].astype(str).str.replace(".", "").astype(int)
        df["mun_id"] = df["mun_id"].astype(int)

        df["nation_id"] = "mex"
        df["nation_name"] = "México"

        return df

class DimZMGeographyPipeline(EasyPipeline):
    @staticmethod
    def steps(params):
        db_connector = Connector.fetch('clickhouse-database', open("../conns.yaml"))

        dtype = {
            "ent_id":       "UInt8",
            "ent_name":     "String",
            "zm_id":        "UInt16",
            "zm_name":      "String",
            "mun_id":       "UInt16",
            "mun_name":     "String",
            "nation_name":  "String",
            "nation_id":    "String"
        }

        transform_step = TransformStep()
        load_step = LoadStep(
            "dim_shared_geography_zm", db_connector, if_exists="drop", dtype=dtype,
            pk=["sun_id", "mun_id"]
        )

        return [transform_step, load_step]
