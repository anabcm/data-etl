import pandas as pd
import requests
from bamboo_lib.models import Parameter, EasyPipeline, PipelineStep
from bamboo_lib.steps import LoadStep
from bamboo_lib.connectors.models import Connector

HEADERS = {
    "Cache-Control": "no-cache",
    "Pragma": "no-cache"
}

class TransformStep(PipelineStep):
    def run_step(self, prev_result, params):
        params = {
            "cube": "inegi_denue",
            "drilldowns": "Month",
            "measures": "Companies",
            "parents": "true"
        }

        BASE_URL = "https://api.datamexico.org/tesseract/data"
        r = requests.get(BASE_URL, params=params, headers=HEADERS)
        data = r.json()["data"]
        df_time = pd.DataFrame(data)
        df_time = df_time.sort_values(by="Month ID", ascending=False)
        df_time.head()

        BASE_URL = "https://dev.datamexico.org/api/stats/pci"

        time = list(df_time["Month ID"])
        agg = 3
        threshold_geo = 300
        threshold_industry = 300

        cube = "inegi_denue"
        level_geo = "Metro Area"
        measure = "Number of Employees Midpoint"

        df = []
        for level_industry in ["Industry Group", "NAICS Industry", "National Industry"]:
            for i, time_id in enumerate(time):
                time_agg = time[i:i + agg]
                n = len(time_agg)
                time_param = ",".join(map(str, time_agg))

                params = {
                    "cube": cube,
                    "Month": time_param,
                    "ranking": "true",
                    "rca": f"{level_geo},{level_industry},{measure}",
                    "threshold": f"{level_industry}:{threshold_industry * n},{level_geo}:{threshold_geo * n}"
                }

                r = requests.get(BASE_URL, params=params, headers=HEADERS)
                data = r.json()["data"]
                df_temp = pd.DataFrame(data)
                df_temp["Time ID"] = time_id
                df_temp["Level"] = level_geo
                df_temp["Latest"] = i == 0

                df.append(df_temp)

        df = pd.concat(df)

        df = df.rename(columns={
            f"{measure} PCI": "pci",
            f"{measure} PCI Ranking": "pci_ranking",
            "National Industry ID": "national_industry_id",
            "Industry Group ID": "industry_group_id",
            "NAICS Industry ID": "naics_industry_id",
            "Time ID": "time_id",
            "Latest": "latest",
            "Level": "level"
        })

        df = df[["national_industry_id", "industry_group_id", "naics_industry_id", "time_id", "latest", "pci", "pci_ranking", "level"]].copy()

        df["latest"] = df["latest"].astype(int)

        for col in "national_industry_id", "industry_group_id", "naics_industry_id":
            df[col] = df[col].fillna(0).astype(int)

        df["national_industry_id"] = df["national_industry_id"].astype(str)

        return df

class ComplexityPCIPipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return []

    @staticmethod
    def steps(params):
        xform_step = TransformStep()
        db_connector = Connector.fetch("clickhouse-database", open("../conns.yaml"))
        dtype = {
            "industry_group_id":     "UInt16",
            "latest":                "UInt8",
            "naics_industry_id":     "UInt32",
            "national_industry_id":  "String",
            "pci":                   "Float32",
            "pci_ranking":           "UInt16",
            "time_id":               "UInt32",
            "level":                 "String"
        }
        load_step = LoadStep(
            "complexity_pci", db_connector, if_exists="drop", 
            pk=["time_id", "latest", "industry_group_id", "naics_industry_id", "national_industry_id"], dtype=dtype
        )
        return [xform_step, load_step]

if __name__ == "__main__":
    pp = ComplexityPCIPipeline()
    pp.run({})