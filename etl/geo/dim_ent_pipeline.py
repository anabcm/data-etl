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
            df.loc[df['altitude'] == '00-{}'.format(i), 'altitude'] = '-00{}'.format(i)

        df["altitude"] = df["altitude"].astype(int)

        df = df.drop(columns=[
            "loc_id", "cve_loc", "loc_name", "zone_id", "cve_loc_full", "cve_mun", "cve_mun_full", "mun_id", "mun_name", "latitude", "longitude", "altitude"
        ])
        df = df.drop_duplicates(subset=["ent_id"]).reset_index().drop(columns="index")

        df["nation_id"] = 1
        df["nation_name"] = "México"

        return df


class DimEntityGeographyPipeline(EasyPipeline):
    @staticmethod
    def steps(params):
        db_connector = Connector.fetch('clickhouse-database', open("../conns.yaml"))

        dtype = {
            "cve_ent":          "String",
            "ent_name":         "String",
            "ent_id":           "UInt8",
            "nation_name":      "String",
            "nation_id":        "UInt8"
        }

        download_step = DownloadStep(
            connector='geo-data',
            connector_path='conns.yaml'
        )
        transform_step = TransformStep()
        load_step = LoadStep(
            "dim_shared_geography_ent", db_connector, if_exists="drop", dtype=dtype,
            pk=['ent_id']
        )

        return [download_step, transform_step, load_step]
