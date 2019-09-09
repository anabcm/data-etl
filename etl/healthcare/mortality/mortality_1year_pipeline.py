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
        _years = ["1994", "1995", "1996", "1997", "1998", "1999", "2000", "2001", "2002", "2003", "2004", "2005",
                  "2006", "2007", "2008", "2009", "2010", "2011", "2012", "2013", "2014", "2015", "2016"]

        # Division per gender (1 year olds)
        df = df[(df["id_indicador"] == 1002000035) | (df["id_indicador"] == 1002000036)| (df["id_indicador"] == 1002000037)]

        # Droping rows related to "Estados Unidos Mexicanos" or national/entity/municipality totals
        df.drop(df.loc[(df["entidad"] == 0) | (df["municipio"] == 0) ].index, inplace=True)

        # Creating news geo ids
        df["mun_id"] = df["entidad"].astype("str").str.zfill(2) + df["municipio"].astype("str").str.zfill(3)

        # Droping used columns, as well years that only had national totals
        df.drop(["entidad", "municipio", "desc_entidad", "desc_municipio", "indicador", "unidad_medida",
                "1990", "1991", "1992", "1993"], axis=1, inplace=True)

        # Division per gender (totals) [1: male, 2: female, 0: unknownj]
        df["id_indicador"].replace({1002000035: 1, 1002000036: 2, 1002000037: 0}, inplace=True)

        # Melt step in order to get file in tidy data format
        df = pd.melt(df, id_vars = ["mun_id", "id_indicador"], value_vars = _years)

        # Renaming columns from spanish to english
        df.rename(columns = {"id_indicador": "gender_id", "variable": "year", "value": "count"}, inplace=True)

        # Groupby step, in order to set count values from NaN to 0
        df = df.groupby(["mun_id", "gender_id", "year"]).sum().reset_index(col_fill="ffill")

        # Setting types
        for item in ["mun_id", "gender_id", "year", "count"]:
            df[item] = df[item].astype(int)

        return df

class ENOEPipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return []

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch("clickhouse-database", open("../conns.yaml"))

        dtype = {
            "mun_id":                   "UInt16",
            "gender_id":                "UInt8",
            "year":                     "UInt16",
            "count":                    "UInt16",
        }

        download_step = DownloadStep(
            connector="mortality-data",
            connector_path="conns.yaml"
        )
        transform_step = TransformStep()
        load_step = LoadStep(
            "inegi_1years_mortality", db_connector, if_exists="append", pk=["mun_id", "gender_id", "year"], dtype=dtype
        )

        return [download_step, transform_step, load_step]