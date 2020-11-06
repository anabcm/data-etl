
import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, PipelineStep
from bamboo_lib.steps import DownloadStep, LoadStep


class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        excel = pd.ExcelFile(prev)

        frames = []
        for year in [2010, 2015]:
            df = pd.read_excel(excel, sheet_name=str(year), header=7)
            df = df[~df["Clave de municipio"].isna()]
            df = df[["Clave de municipio", "Coeficiente de Gini", "Razón de ingreso 1"]]
            df["year"] = year
            frames.append(df)

        df_concat = pd.concat(frames)
        df_concat = df_concat.rename(columns={
            "Clave de municipio": "mun_id",
            "Coeficiente de Gini": "gini",
            "Razón de ingreso 1": "income_rate"
        })
        df_concat["mun_id"] = df_concat["mun_id"].astype(int)
        for col in ["gini", "income_rate"]:
            df_concat[col] = df_concat[col].replace("n.d.", pd.np.nan).astype(float)

        return df_concat

class CONEVALGiniPipeline(EasyPipeline):
    @staticmethod
    def steps(params):
        db_connector = Connector.fetch("clickhouse-database", open("../conns.yaml"))
        dtype = {
            "gini":         "Float32",
            "income_rate":  "Float32",
            "mun_id":       "UInt16",
            "year":         "UInt16"
        }

        download_step = DownloadStep(
            connector="gini-mun-data",
            connector_path="conns.yaml"
        )

        transform_step = TransformStep()
        load_step = LoadStep(
            "coneval_gini_mun", db_connector, if_exists="drop", pk=["mun_id", "year"], dtype=dtype,
            nullable_list=["income_rate"]
        )

        return [download_step, transform_step, load_step]