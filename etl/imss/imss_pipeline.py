import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, PipelineStep, Parameter
from bamboo_lib.steps import DownloadStep, LoadStep

class TransformStep(PipelineStep):
    def run_step(self, prev, params):

        # IMSS
        df = pd.read_csv(prev, low_memory=False, encoding="latin-1", delimiter="|", chunksize=10**5)
        df = pd.concat(df)

        df.columns = df.columns.str.lower().str.strip()

        df = df.drop(columns=["cve_subdelegacion", "sector_economico_1", "sector_economico_2", 
                "cve_delegacion", "masa_sal_teu", "masa_sal_tec", "masa_sal_tec", "masa_sal_tpc",
                "masa_sal_tpu", "masa_sal_tpc", "ta_sal", "teu_sal", "tec_sal", "tpu_sal", "tpc_sal"])

        df = df.rename(columns={"rango_edad": "age_range", 
                                "cve_municipio": "mun_id_imss", 
                                "sexo": "sex", 
                                "tamaño_patron": "pattern_size", 
                                "rango_uma": "uma_range", 
                                "no_trabajadores": "non_workers",
                                "rango_salarial": "salary_range", 
                                "cve_entidad": "ent_id"})

        # "pattern_size", "salary_range", "uma_range", NaNs values have a category
        for col in ["sector_economico_4", "age_range", "salary_range", "sex", "pattern_size", "salary_range", "uma_range"]:
            df[col] = df[col].fillna(0)

        df.loc[df["mun_id_imss"].isna(), "mun_id_imss"] = df.loc[df["mun_id_imss"].isna(), "ent_id"].astype(int).astype(str) + "000"

        df["count"] = 1
        df["age_range"] = df["age_range"].astype(str).str.upper().str.replace("E", "").astype(int)
        df["uma_range"] = df["uma_range"].astype(str).str.upper().str.replace("W", "").astype(int)
        df["pattern_size"] = df["pattern_size"].astype(str).str.upper().str.replace("S", "").astype(int)
        df["salary_range"] = df["salary_range"].astype(str).str.upper().str.replace("W", "").astype(int)

        for col in ["sex", "sector_economico_4"]:
            df[col] = df[col].astype(int)

        df = df.groupby(["age_range", "mun_id_imss", "ent_id", "salary_range", "sector_economico_4",
                        "sex", "pattern_size", "uma_range"]).sum().reset_index()

        df["salary"] = df["masa_sal_ta"] / df["count"]

        # Data dictonary from IMSS
        df1 = pd.read_excel(open("https://storage.googleapis.com/datamexico-data/imss/diccionario_de_datos_1.xlsx", "rb"),
                    sheet_name="entidad-municipio", header=1) 

        df1.rename(columns={"cve_municipio": "mun_id_imss",
                            "descripción municipio": "mun_id",
                            "cve_entidad": "ent_id"}, inplace=True)

        df1["mun_id"].replace({
            "Batopilas": "Batopilas de Manuel Gómez Morín", 
            "Dolores Hidalgo Cuna de la Independencia": "Dolores Hidalgo Cuna de la Independencia Nacional", 
            "Jonacatepec": "Jonacatepec de Leandro Valle", 
            "Villa de Tututepec de Melchor Ocampo": "Villa de Tututepec", 
            "Heroica Villa Tezoatlán de Segura y Luna": "Heroica Villa Tezoatlán de Segura y Luna, Cuna de la Independencia de Oaxaca",
            "Isla de Cedros": "Ensenada", "Santa Ana Pacueco": "Pénjamo"}, inplace=True)

        df1 = df1[["mun_id_imss", "mun_id", "ent_id"]]

        # Data Mexico DW
        df2 = pd.read_csv("https://storage.googleapis.com/datamexico-data/imss/Municipality.csv")

        df2 = df2[["Municipality ID", "Municipality"]].copy()
        df2.columns = ["mun_id", "mun_name"]

        df1["mun_id"].replace(dict(zip(df2["mun_name"], df2["mun_id"])), inplace=True)

        df1["ent_id"] = df1["ent_id"].astype(str)

        df1.loc[df1["mun_id"].isna(), "mun_id"] = df1.loc[df1["mun_id"].isna(), "ent_id"] + "000"

        df["mun_id_imss"] = df["mun_id_imss"].map(dict(zip(df1["mun_id_imss"], df1["mun_id"])))
        df.loc[df["mun_id_imss"].isna(), "mun_id_imss"] = df.loc[df["mun_id_imss"].isna(), "ent_id"].astype(int).astype(str) + "000"
        df["mun_id_imss"] = df["mun_id_imss"].astype(int)

        df.drop(columns=["ent_id"],inplace=True)

        df["month_id"] = params.get("year") + params.get("month")
        df["month_id"] = df["month_id"].astype(int)

        df.rename(columns={"mun_id_imss": "mun_id",
                           "sector_economico_4": "level_4_id"}, inplace=True)

        return df

class IMSSPipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return [
            Parameter(label="month", name="month", dtype=str),
            Parameter(name="year", dtype=str)
        ]

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch("clickhouse-database", open("../conns.yaml"))

        dtypes = {
            "month_id":               "UInt32",
            "mun_id":                 "UInt16",
            "age_range":              "UInt8",
            "salary_range":           "UInt8",
            "level_4_id":             "UInt16",
            "sex":                    "UInt8",
            "pattern_size":           "UInt8",
            "uma_range":              "UInt8",
            "asegurados":             "UInt32",
            "non_workers":            "UInt32",
            "ta":                     "UInt16",
            "teu":                    "UInt16",
            "tec":                    "UInt16",
            "tpu":                    "UInt16",
            "tpc":                    "UInt16",
            "masa_sal_ta":            "UInt32",
            "count":                  "UInt16",
            "salary":                 "UInt32"
        }

        download_step = DownloadStep(
            connector="imss-data",
            connector_path="conns.yaml"
        )

        xform_step = TransformStep()
        load_step = LoadStep(
            "imss", db_connector, if_exists="append", 
            pk=["mun_id", "month_id"], dtype=dtypes
        )

        return [download_step, xform_step, load_step]

if __name__ == "__main__":
    for year in range(2019, 2020 + 1):
        for month in range(1, 12 + 1):
            pp = IMSSPipeline()
            pp.run({"year": str(year),
                    "month": str(month).zfill(2)})
