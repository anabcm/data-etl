import pandas as pd
import numpy as np
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, PipelineStep, Parameter
from bamboo_lib.steps import DownloadStep, LoadStep


class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        industrial_parks = pd.read_excel(prev[0])
        dim_names = pd.read_csv(prev[1], sep=";", encoding="utf-8").to_dict(orient="list")

        #Create dictionary for park names
        dicto_names = dict(zip(dim_names["park_name"], dim_names["park_id"]))
        
        dicto_names_new = {}
        for park_name, id_val in dicto_names.items():
            aux_park_name = park_name.replace('\n', '')
            dicto_names_new[aux_park_name] = id_val

        dicto_missing = {
            'Carretera Saltillo - Torreon': 793,
            'CPA Logistics Center Huehuetoca': 838
        }
        
        df = industrial_parks
        df = df[["CVEGEO", "NOMBRE", "MASTER_TIPO_DE_ASENTAMIENTO"]].copy()      
        df = df.drop_duplicates()   

        park_type = {
            "Parque industrial": 1,
            "Zona industrial": 2,
            "Parque industrial en construcci�n/desarrollo": 3,
            "Terreno industrial": 4,
            "Edificio industrial": 5,
            "Microparque": 6,
            "Nave industrial": 7,
            "Pol�gono industrial": 8,
            "Cl�ster": 9,
            "Campus universitario": 10,
            "Lote industrial": 11
        }

        df["MASTER_TIPO_DE_ASENTAMIENTO"] = df["MASTER_TIPO_DE_ASENTAMIENTO"].replace(park_type)

        df["NOMBRE"] = df["NOMBRE"].replace(dicto_names_new)
        df["NOMBRE"] = df["NOMBRE"].replace(dicto_missing)
        df.columns = ["mun_id", "park_name_id", "park_type_id"]

        df["count_parks"] = 1

        return df

class IndustrialParkPipeline(EasyPipeline):
    @staticmethod
    def steps(params):
        db_connector = Connector.fetch(
            "clickhouse-database", open("../conns.yaml"))

        dtypes = {
            "mun_id":           "UInt16",
            "park_name_id":     "UInt16",
            "park_type_id":     "UInt8",
            "count_parks":      "UInt8"
        }

        download_step = DownloadStep(
            connector=["dataset1_industrial_parks", "names_industrial_parks"],
            connector_path="conns.yaml",
            force=True
        )

        transform_step = TransformStep()
        
        load_step = LoadStep(
            "industrial_parks", db_connector, if_exists="drop",
            pk=["mun_id", "park_name_id", "park_type_id"], 
            dtype=dtypes
        )

        return [download_step, transform_step, load_step]

if __name__ == "__main__":
    industrial_parks = IndustrialParkPipeline()
    industrial_parks.run({})