
import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import DownloadStep
from bamboo_lib.steps import LoadStep

class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        excel = pd.ExcelFile('https://storage.googleapis.com/datamexico-data/coneval/GINI_Ent.xlsx')

        sheets = {'Coeficiente de Gini 2008-2014': ['ent_id', '2008', '2010', '2012', '2014'],
                'Coeficiente de Gini 2016-2018': ['ent_id', '2016', '2018']}
        df = pd.DataFrame()
        df_temp = pd.DataFrame()

        for sheet, cols in sheets.items():
            df_temp = pd.read_excel(excel, sheet_name=sheet, header=6)
            df_temp.dropna(axis=1, how='all', inplace=True)
            df_temp.dropna(axis=0, how='any', inplace=True)
            df_temp.columns = cols
            df_temp = df_temp.melt(id_vars='ent_id', var_name='year', value_name='gini').copy()
            df_temp = df_temp.loc[df_temp['ent_id'] != 'Estados Unidos Mexicanos'].copy()
            url = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vSDaqIIMI56NCwzU1fJxz6erC474xtqJBytaBaqVJS6b5Op7nr1p_sE1Fq4XKVaNdDjoz-yOzX1rRj6/pub?output=xlsx'
            ent_codes = pd.read_excel(url, sheet_name='federal_entities')
            df_temp['ent_id'].replace(dict(zip(ent_codes['name'], ent_codes['code'])), inplace=True)
            df = df.append(df_temp)
        
        df['ent_id'] = df['ent_id'].astype('int')
        df['year'] = df['year'].astype('int')
        df['gini'] = df['gini'].astype('float')

        return df

class CONEVALGiniPipeline(EasyPipeline):
    @staticmethod
    def steps(params):
        db_connector = Connector.fetch("clickhouse-database", open("../conns.yaml"))
        dtype = {
            "gini":         "Float32",
            "ent_id":       "UInt8",
            "year":         "UInt16"
        }

        transform_step = TransformStep()
        load_step = LoadStep(
            "coneval_gini_ent", db_connector, if_exists="drop", pk=["ent_id", "year"], dtype=dtype
        )

        return [transform_step, load_step]