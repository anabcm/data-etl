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
            "loc_name":     "String"
        }

        transform_step = TransformStep()
        load_step = LoadStep(
            "dim_shared_geography_sun", db_connector, if_exists="drop", dtype=dtype,
            pk=["sun_id", "mun_id"], nullable_list=["loc_id", "loc_name"]
        )

        return [transform_step, load_step]
