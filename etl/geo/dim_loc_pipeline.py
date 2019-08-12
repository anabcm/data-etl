import numpy as np
import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import DownloadStep
from bamboo_lib.steps import LoadStep


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

        df["ent_id"] = df["cve_ent"].astype(int)
        df["mun_id"] = df["cve_mun_full"].astype(int)
        df["loc_id"] = df["cve_loc_full"].astype(int)

        # Fix weird type issue with altitude values
        for i in list(range(1, 10)):
            df.loc[df["altitude"] == "00-{}".format(i), "altitude"] = "-00{}".format(i)

        df_other_loc = []
        for item in df[["ent_id", "ent_name", "mun_id", "mun_name", "cve_mun_full", "cve_mun", "cve_ent"]].drop_duplicates().reset_index().itertuples():

            df_other_loc.append({
                "loc_id": item.mun_id * 10000,
                "cve_loc_full": item.cve_mun_full + "0000",
                "cve_loc": "0000",
                "altitude": np.nan,
                "longitude": np.nan,
                "latitude": np.nan,
                "zone_id": 0,
                "loc_name": "Otras localidades de {}".format(item.mun_name),
                "ent_id": item.ent_id,
                "ent_name": item.ent_name,
                "mun_id": item.mun_id,
                "mun_name": item.mun_name,
                "cve_mun_full": item.cve_mun_full,
                "cve_mun": item.cve_mun,
                "cve_ent": item.cve_ent,
            })

        df_other_loc = pd.DataFrame(df_other_loc)
        df = df.append(df_other_loc)
        df["altitude"] = df["altitude"].astype(float)
        df["zone_id"] = df["zone_id"].astype(float)

        df["nation_id"] = "mex"
        df["nation_name"] = "México"

        return df


class DimLocationGeographyPipeline(EasyPipeline):
    @staticmethod
    def steps(params):
        db_connector = Connector.fetch("clickhouse-database", open("../conns.yaml"))

        dtype = {
            "cve_ent":          "String",
            "cve_mun":          "String",
            "cve_loc":          "String",
            "cve_mun_full":     "String",
            "cve_loc_full":     "String",
            "ent_name":         "String",
            "mun_name":         "String",
            "loc_name":         "String",
            "latitude":         "Float64",
            "longitude":        "Float64",
            "altitude":         "Float64",
            "ent_id":           "UInt8",
            "mun_id":           "UInt16",
            "loc_id":           "UInt32",
            "nation_name":      "String",
            "nation_id":        "String"
        }

        download_step = DownloadStep(
            connector="geo-data",
            connector_path="conns.yaml"
        )
        transform_step = TransformStep()
        load_step = LoadStep(
            "dim_shared_geography", db_connector, if_exists="drop", dtype=dtype,
            pk=["ent_id", "mun_id", "loc_id"], nullable_list=["altitude", "latitude", "longitude"]
        )

        return [download_step, transform_step, load_step]
