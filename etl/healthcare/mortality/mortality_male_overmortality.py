import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import DownloadStep
from bamboo_lib.steps import LoadStep


class TransformStep(PipelineStep):
    def run_step(self, prev, params):

        # Renaming columns from first quarter to match the rest of the year
        df = pd.read_excel(prev, index_col=None, header=0)

        # Columns with columns besides annual totals
        _years = [str(i) for i in range(1994,2017)]

        # Rows related to overmortality (number of men dying over 100 females)
        df = df.loc[(df["id_indicador"] == 6200240468)]

        # Droping rows related to "Estados Unidos Mexicanos" or national/entity totals
        df.drop(df.loc[(df["entidad"] == 0)].index, inplace=True)

        # Creating news geo ids
        df["ent_id"] = df["entidad"].astype("str").str.zfill(2)

        # Droping used columns, as well years that only had national totals
        df.drop(["entidad", "municipio", "desc_entidad", "desc_municipio", "indicador", "unidad_medida",
                "1990", "1991", "1992", "1993"], axis=1, inplace=True)

        # Melt step in order to get file in tidy data format
        df = pd.melt(df, id_vars = ["ent_id"], value_vars = _years)

        # Renaming columns from spanish to english
        df.rename(columns = {"variable": "year", "value": "male_overmortality_index"}, inplace=True)

        # Droping last columns
        df.drop(["id_indicador", "code"], axis=1, inplace=True)

        # Setting types
        for item in ["ent_id", "year"]:
            df[item] = df[item].astype(int)
        df["male_overmortality_index"] = df["male_overmortality_index"].astype(float)

        return df

class ENOEPipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return []

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch("clickhouse-database", open("../conns.yaml"))

        dtype = {
            "ent_id":                           "UInt8",
            "year":                             "UInt16",
            "male_overmortality_index":         "Float32"
        }

        download_step = DownloadStep(
            connector="mortality-data",
            connector_path="conns.yaml"
        )
        transform_step = TransformStep()
        load_step = LoadStep(
            "inegi_overmortality_mortality", db_connector, if_exists="append", pk=["ent_id", "year"], dtype=dtype
        )

        return [download_step, transform_step, load_step]