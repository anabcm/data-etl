import pandas as pd
import requests
from bamboo_lib.models import Parameter, EasyPipeline, PipelineStep
from bamboo_lib.steps import LoadStep
from bamboo_lib.connectors.models import Connector

class TransformStep(PipelineStep):
    def run_step(self, prev_result, params):
        params = {
            "cube": "inegi_denue",
            "drilldowns": "Month",
            "measures": "Companies",
            "parents": "true"
        }

        BASE_URL = "https://api.datamexico.org/tesseract/data"
        r = requests.get(BASE_URL, params=params)
        data = r.json()["data"]
        df_time = pd.DataFrame(data)
        df_time = df_time.sort_values(by="Month ID", ascending=False)
        df_time.head()

        BASE_URL = "https://dev.datamexico.org/api/stats/eci"

        time = list(df_time["Month ID"])
        agg = 3
        threshold_geo = 300
        threshold_industry = 300

        cube = "inegi_denue"
        level_industry = "NAICS Industry"
        measure = "Number of Employees Midpoint"

        df = []
        for level_geo in ["State", "Metro Area", "Municipality"]:
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

                r = requests.get(BASE_URL, params=params)
                data = r.json()["data"]
                df_temp = pd.DataFrame(data)
                df_temp["Time ID"] = time_id
                df_temp["Level"] = level_geo
                df_temp["Latest"] = i == 0

                df_temp = df_temp.drop(columns=[level_geo])
                df.append(df_temp)

        df = pd.concat(df)

        df = df.rename(columns={
            f"{measure} ECI": "eci",
            f"{measure} ECI Ranking": "eci_ranking",
            "State ID": "ent_id",
            "Metro Area ID": "zm_id",
            "Municipality ID": "mun_id",
            "Time ID": "time_id",
            "Latest": "latest",
            "Level": "level"
        })
        df["latest"] = df["latest"].astype(int)

        for col in ["ent_id", "zm_id", "mun_id"]:
            df[col] = df[col].fillna(0).astype(int)

        """df['level'].replace({"State": 1,
                             "Metro Area": 2,
                             "Municipality": 3}, 
                             inplace=True)"""
        
        return df

class ComplexityECIPipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return []

    @staticmethod
    def steps(params):
        xform_step = TransformStep()
        db_connector = Connector.fetch("clickhouse-database", open("../conns.yaml"))
        dtype = {
            "eci":          "Float32",
            "eci_ranking":  "UInt16",
            "ent_id":       "UInt8",
            "latest":       "UInt8",
            "level":        "UInt8",
            "mun_id":       "UInt16",
            "time_id":      "UInt32",
            "zm_id":        "UInt32"
        }
        load_step = LoadStep(
            "complexity_eci", db_connector, if_exists="drop", pk=["time_id", "latest", "ent_id", "zm_id", "mun_id"], dtype=dtype
        )
        return [xform_step, load_step]

if __name__ == "__main__":
    pp = ComplexityECIPipeline()
    pp.run({})