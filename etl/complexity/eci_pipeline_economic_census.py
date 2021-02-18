
import requests
import pandas as pd
from bamboo_lib.models import Parameter, EasyPipeline, PipelineStep
from bamboo_lib.steps import LoadStep
from bamboo_lib.connectors.models import Connector

class TransformStep(PipelineStep):
    def run_step(self, prev_result, params):
        params = {
            "cube": "inegi_economic_census_additional",
            "drilldowns": "Year",
            "measures": "Economic Unit",
            "parents": "true"
        }

        BASE_URL = "https://dev-api.datamexico.org/tesseract/data"
        r = requests.get(BASE_URL, params=params)
        data = r.json()["data"]
        df_time = pd.DataFrame(data)
        df_time = df_time.sort_values(by="Year", ascending=False)

        BASE_URL = "https://dev.datamexico.org/api/stats/eci"

        time = list(df_time["Year"])
        agg = 1
        threshold_geo = 300
        threshold_industry = 300

        cube = "inegi_economic_census_additional"
        level_industry = ""
        measure = "Total Gross Production"
        df_all = pd.DataFrame()

        for level_industry in ["Sector", "Subsector", "Industry Group"]:

            df = []
            for level_geo in ["State", "Municipality"]:
                print('Current Industry: {}, Current Geo: {}'.format(level_industry, level_geo))
                try:
                    for i, time_id in enumerate(time):
                        time_agg = time[i:i + agg]
                        n = len(time_agg)
                        time_param = ",".join(map(str, time_agg))

                        params = {
                            "cube": cube,
                            "Year": time_param,
                            "ranking": "true",
                            "rca": f"{level_geo},{level_industry},{measure}",
                            "threshold": f"{level_industry}:{threshold_industry * n},{level_geo}:{threshold_geo * n}"
                        }

                        r = requests.get(BASE_URL, params=params)
                        data = r.json()["data"]
                        df_temp = pd.DataFrame(data)
                        df_temp["Time ID"] = time_id
                        # df_temp["Level"] = level_geo
                        df_temp["Latest"] = i == 0
                        # df_temp["Industry Level"] = level_industry
                        df_temp["Level"] = '{} - {}'.format(level_geo, level_industry)

                        df_temp = df_temp.drop(columns=[level_geo])
                        df.append(df_temp)
                except Exception as e:
                    """Geo - Industry combination not present on cube"""
                    continue

            df = pd.concat(df)

            df = df.rename(columns={
                f"{measure} ECI": "eci",
                f"{measure} ECI Ranking": "eci_ranking",
                "State ID": "ent_id",
                "Metro Area ID": "zm_id",
                "Municipality ID": "mun_id",
                "Time ID": "time_id",
                "Latest": "latest",
                "Level": "level",
                "Industry Level": 'industry_level'
            })
            df["latest"] = df["latest"].astype(int)
            df_all = df_all.append(df, sort=False)
        df = df_all.copy()
        df_all = []

        for col in ["ent_id", "mun_id"]:
            df[col] = df[col].fillna(0).astype(int)

        return df

class ComplexityECIPipeline(EasyPipeline):
    @staticmethod
    def steps(params):
        xform_step = TransformStep()
        db_connector = Connector.fetch("clickhouse-database", open("../conns.yaml"))
        dtype = {
            "eci":          "Float32",
            "eci_ranking":  "UInt16",
            "ent_id":       "UInt8",
            "latest":       "UInt8",
            "level":        "String",
            "mun_id":       "UInt16",
            "time_id":      "UInt32",
        }
        load_step = LoadStep(
            "complexity_eci_economic_census", db_connector, if_exists="drop", pk=["time_id", "level", "latest", 
            "ent_id", "mun_id"], dtype=dtype
        )
        return [xform_step, load_step]

if __name__ == "__main__":
    pp = ComplexityECIPipeline()
    pp.run({})
