import pandas as pd
import unidecode
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import DownloadStep
from bamboo_lib.steps import LoadStep

def slug_parser(txt):
    slug = txt.lower().replace(" ", "-")
    slug = unidecode.unidecode(slug)

    for char in ["]", "[", "(", ")"]:
        slug = slug.replace(char, "")

    return slug

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

        df1 = pd.read_csv("https://storage.googleapis.com/datamexico-data/geo/AGEEML_2019517152111.csv", low_memory=False, chunksize=10**4)
        df1 = pd.concat(df1)

        df1.columns = df1.columns.str.lower()

        df1["temp_cve_ent"] = df1["cve_ent"].astype(str).str.zfill(2)
        df1["temp_cve_mun"] = df1["cve_mun"].astype(str).str.zfill(3)
        df1["temp_cve_loc"] = df1["cve_loc"].astype(str).str.zfill(4)

        df1["mun_id"] = df1["temp_cve_ent"] + df1["temp_cve_mun"]
        df1["loc_id"] = df1["temp_cve_ent"] + df1["temp_cve_mun"] + df1["temp_cve_loc"]

        df1.rename(columns={"nom_loc": "loc_name", "nom_mun": "mun_name"}, inplace=True)

        df1["mun_id"] = df1["mun_id"].astype(int)
        df1["loc_id"] = df1["loc_id"].astype(int)

        df1 = df1[["mun_id", "mun_name", "loc_id", "loc_name"]].drop_duplicates().reset_index()

        df_other_loc = []
        for item in df1.itertuples():

            df_other_loc.append({
                "loc_id": item.mun_id * 10000,
                "loc_name": "Otras localidades de {}".format(item.mun_name),
                "mun_id": item.mun_id
            })

        df_other_loc = pd.DataFrame(df_other_loc)
        df1 = df1.append(df_other_loc)

        df1 = df1.drop(columns=["mun_name"])

        df_output = df1.merge(df, on="mun_id")
        df_output = df_output[["ent_id", "ent_name", "zm_id", "zm_name", "zm_slug", "mun_id", "mun_name", "loc_name", "loc_id"]]
        df_output = df_output.drop_duplicates()

        return df_output

class DimZMLocationGeographyPipeline(EasyPipeline):
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
            "loc_id":       "UInt32",
            "loc_name":     "String",
            "nation_name":  "String",
            "nation_id":    "String"
        }

        transform_step = TransformStep()
        load_step = LoadStep(
            "dim_shared_geography_zm_loc", db_connector, if_exists="drop", dtype=dtype,
            pk=["zm_id", "mun_id", "loc_id"]
        )

        return [transform_step, load_step]
