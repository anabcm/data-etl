import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import DownloadStep
from bamboo_lib.steps import LoadStep

class ExtractStep(PipelineStep):
    def run_step(self, prev, params):

        # Data Dictonary from IMSS
        xls = pd.ExcelFile(prev)

        # Economic sector 1 ,2 and 4 Sheets from Data dictonary from IMSS
        df1 = pd.read_excel(xls, "sector 1", header=1)
        df2 = pd.read_excel(xls, "sector 2", header=1)
        df3 = pd.read_excel(xls, "sector 4", header=1)
        df = pd.merge(df1, df2, on=["sector_economico_1"])
        df = pd.merge(df, df3, on=["sector_economico_1", "sector_economico_2_2pos"])
        df = df.drop(columns=["sector_economico_2_x", "sector_economico_2_y", "sector_economico_4"])
        df = df.rename(columns={"sector_economico_1" : "level_1_id", "sector_economico_2_2pos" : "level_2_id", "sector_economico_4_4_pos" : 
                                "level_4_id", "descripción sector_economico_1": "level_1_name", "descripción sector_economico_2" : "level_2_name", 
                                "descripción sector_economico_4" : "level_4_name"})
        df["level_2_id"] = df["level_2_id"].astype(int)

        return df

class DimEconomicSectorPipeline(EasyPipeline):
    @staticmethod
    def steps(params):
        db_connector = Connector.fetch("clickhouse-database", open("../conns.yaml"))
        dtype = {
            "level_1_id":   "UInt8", 
            "level_1_name": "String", 
            "level_2_id":   "UInt8", 
            "level_2_name": "String",
            "level_4_id":   "UInt16", 
            "level_4_name": "String"
        }

        download_step = DownloadStep(
            connector="imss-dimension",
            connector_path="conns.yaml"
        )

        extract_step = ExtractStep()
        load_step = LoadStep(
            "imss_economic_sector", db_connector, if_exists="drop", dtype=dtype,
            pk=["level_4_id", "level_2_id", "level_1_id"]
        )

        return [download_step, extract_step, load_step]

if __name__ == "__main__":
    pp = DimEconomicSectorPipeline()
    pp.run({})
