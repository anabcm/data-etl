import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import DownloadStep
from bamboo_lib.steps import LoadStep

from etl.geo.shared import STATE_REPLACE, MISSING_MUNICIPALITY
from etl.geo.shared import slug_parser

class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        df = pd.read_csv(prev, low_memory=False, chunksize=10**4)
        df = pd.concat(df)
        df.columns = df.columns.str.lower()

        df["temp_cve_ent"] = df["cve_ent"].astype(str).str.zfill(2)
        df["temp_cve_mun"] = df["cve_mun"].astype(str).str.zfill(3)
        df["temp_cve_loc"] = df["cve_loc"].astype(str).str.zfill(4)

        df["temp_cve_mun_full"] = df["temp_cve_ent"] + df["temp_cve_mun"]
        df["temp_cve_loc_full"] = df["temp_cve_ent"] + df["temp_cve_mun"] + df["temp_cve_loc"]

        # Create location schema
        df = df[[
            "temp_cve_ent", "temp_cve_mun", "temp_cve_loc", "temp_cve_mun_full", "temp_cve_loc_full",
            "nom_ent", "nom_mun", "nom_loc", "ámbito", "lat_decimal", "lon_decimal", "altitud"
        ]]

        df.rename(columns={
            "temp_cve_ent": "cve_ent",
            "temp_cve_mun": "cve_mun",
            "temp_cve_loc": "cve_loc",
            "temp_cve_mun_full": "cve_mun_full",
            "temp_cve_loc_full": "cve_loc_full",
            "nom_ent": "ent_name",
            "nom_mun": "mun_name",
            "nom_loc": "loc_name",
            "ámbito": "zone_id",
            "lat_decimal": "latitude",
            "lon_decimal": "longitude",
            "altitud": "altitude"
        }, inplace=True)

        zones = {"U": 1, "R": 2}
        df["zone_id"] = df["zone_id"].replace(zones)

        df = df.loc[~df["cve_ent"].isin(['CD', 'CO', 'CQ', 'CT', 'CY', 'QY'])].copy()
        df["ent_id"] = df["cve_ent"].astype(int)
        df["mun_id"] = df["cve_mun_full"].astype(int)
        df["loc_id"] = df["cve_loc_full"].astype(int)

        # Fix weird type issue with altitude values
        for i in list(range(1, 10)):
            df.loc[df["altitude"] == "00-{}".format(i), "altitude"] = "-00{}".format(i)

        df["altitude"] = df["altitude"].astype(int)

        df = df.drop(columns=[
            "loc_id", "cve_loc", "loc_name", "zone_id", "cve_loc_full", "latitude", "longitude", "altitude"
        ])
        df = df.drop_duplicates(subset=["mun_id"]).reset_index().drop(columns="index")

        df["nation_id"] = "mex"
        df["nation_name"] = "México"
        df["nation_slug"] = "mexico"

        ent_iso2 = {
            1: "ag", 2: "bc", 3: "bs", 4: "cm", 5: "co", 6: "cl", 7: "cs", 8: "ch",
            9: "cx", 10: "dg", 11: "gt", 12: "gr", 13: "hg", 14: "jc", 15: "em", 16: "mi",
            17: "mo", 18: "na", 19: "nl", 20: "oa", 21: "pu", 22: "qt", 23: "qr", 24: "sl",
            25: "si", 26: "so", 27: "tb", 28: "tm", 29: "tl", 30: "ve", 31: "yu", 32: "za"
        }

        ent_iso3 = {
            1: "agu", 2: "bcn", 3: "bcs", 4: "cam", 5: "coa", 6: "col", 7: "chp", 8: "chh",
            9: "cmx", 10: "dur", 11: "gua", 12: "gro", 13: "hid", 14: "jal", 15: "mex", 16: "mic",
            17: "mor", 18: "nay", 19: "nle", 20: "oax", 21: "pue", 22: "que", 23: "roo", 24: "slp",
            25: "sin", 26: "son", 27: "tab", 28: "tam", 29: "tla", 30: "ver", 31: "yuc", 32: "zac"
        }

        df["ent_iso2"] = df["ent_id"].replace(ent_iso2)
        df["ent_iso3"] = df["ent_id"].replace(ent_iso3)

        df["ent_slug"] = (df["ent_name"] + " " + df["ent_iso2"]).apply(slug_parser)
        df["mun_slug"] = (df["mun_name"] + " mun " + df["ent_iso2"]).apply(slug_parser)

        df = df.append(MISSING_MUNICIPALITY, ignore_index=True)
        df["ent_name"].replace(STATE_REPLACE, inplace=True)

        return df


class DimMunicipalityGeographyPipeline(EasyPipeline):
    @staticmethod
    def steps(params):
        db_connector = Connector.fetch("clickhouse-database", open("../conns.yaml"))

        dtype = {
            "cve_ent":          "String",
            "cve_mun":          "String",
            "cve_mun_full":     "String",
            "ent_name":         "String",
            "mun_name":         "String",
            "ent_id":           "UInt8",
            "mun_id":           "UInt16",
            "nation_name":      "String",
            "nation_id":        "String"
        }

        download_step = DownloadStep(
            connector="geo-data-census",
            connector_path="conns.yaml"
        )
        transform_step = TransformStep()
        load_step = LoadStep(
            "dim_shared_geography_mun_census", db_connector, if_exists="drop", dtype=dtype,
            pk=["ent_id", "mun_id"], nullable_list=["ent_iso2", "ent_iso3", "ent_slug", "mun_slug", "nation_slug"]
        )

        return [download_step, transform_step, load_step]

if __name__ == "__main__":
    pp = DimMunicipalityGeographyPipeline()
    pp.run({})