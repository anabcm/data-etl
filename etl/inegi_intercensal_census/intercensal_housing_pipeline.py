
import pandas as pd
from bamboo_lib.models import PipelineStep, AdvancedPipelineExecutor
from bamboo_lib.models import Parameter, BasePipeline
from bamboo_lib.connectors.models import Connector
from bamboo_lib.steps import DownloadStep
from bamboo_lib.steps import LoadStep
from bamboo_lib.helpers import grab_connector

class ReadStep(PipelineStep):
    def run_step(self, prev, params):
        # intercensal census data
        df = prev
        # data to replace
        url = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vR08Js9Sh4nNTMe5uBcsDUFedG5MOjIf90p6EHAr1_sWY5kpnI3xUvyPHzQpTEUrXz1pskaoc0uyea6/pub?output=xlsx'
        data = {}
        for col in ['pisos', 'techos', 'paredes']:
            data[col] = pd.read_excel(url, sheet_name=col, encoding='latin-1', dtype='str')
        return df, data

class CleanStep(PipelineStep):
    def run_step(self, prev, params):
        df, data = prev[0], prev[1]
        # preformat
        df.columns = df.columns.str.lower()
        # municipality level
        df['mun_id'] = (df.ent + df.mun).astype('int')
        # location level
        df['loc_id'] = (df.ent + df.mun + df.loc50k).astype('int')
        # column type conversion
        df['ingtrhog'] = df['ingtrhog'].astype('float')
        df.ingtrhog = df.ingtrhog.fillna(0).round(0).astype('int64')
        return df, data

class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        df, data = prev[0], prev[1]
        # replace, select and group data
        for col in data.keys():
            df[col] = df[col].replace(dict(zip(data[col]['prev_id'], data[col]['id'])))
        df = df[['mun_id', 'loc_id', 'cobertura', 'ingtrhog', 'pisos', 'techos', 'paredes', 'forma_adqui', 'deuda', 'factor']]
        df = df.groupby(['mun_id', 'loc_id', 'cobertura', 'pisos', 'techos', 'paredes', 'forma_adqui', 'deuda', 'factor']).sum().reset_index(col_fill='ffill').rename(columns={'factor': 'population', 'pisos': 'floor', 'paredes': 'wall', 'techos': 'roof', 'forma_adqui': 'acquisition', 'deuda': 'debt', 'ingtrhog': 'income'})
        return df

class IncomeIntervalStep(PipelineStep):
    def run_step(self, prev, params):
        df = prev
        # income interval
        url = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vR08Js9Sh4nNTMe5uBcsDUFedG5MOjIf90p6EHAr1_sWY5kpnI3xUvyPHzQpTEUrXz1pskaoc0uyea6/pub?output=xlsx'
        income = pd.read_excel(url, sheet_name='income', encoding='latin-1')
        for ing in df.income.unique():
            for level in range(income.shape[0]):
                if (ing >= income.interval_lower[level]) & (ing < income.interval_upper[level]):
                    df.income = df.income.replace(ing, income.id[level])
                    break
                if ing >= income.interval_upper[income.shape[0]-1]:
                    df.income = df.income.replace(ing, income.id[income.shape[0]-1])
                    break
        df.income = df.income.astype('int')
        return df

class CoveragePipeline(BasePipeline):
    @staticmethod
    def pipeline_id():
        return 'program-coverage-pipeline-temp'

    @staticmethod
    def name():
        return 'Program Coverage Pipeline temp'

    @staticmethod
    def description():
        return 'Processes information from Mexico'

    @staticmethod
    def website():
        return 'http://datawheel.us'

    @staticmethod
    def parameter_list():
        return [
            Parameter(label='Source connector', name='source-connector', dtype=str, source=Connector)
        ]

    @staticmethod
    def run(params, **kwargs):
        # Use of connectors specified in the conns.yaml file
        db_connector = Connector.fetch('clickhouse-database', open('etl/conns.yaml'))
        dtype = {
            'mun_id':      'String', 
            'loc_id':      'String', 
            'cobertura':   'String', 
            'floor':       'String', 
            'roof':        'String', 
            'wall':        'String', 
            'acquisition': 'String',
            'debt':        'String', 
            'population':  'String', 
            'income':      'UInt32'
        }

        download_step = DownloadStep(
            connector='housing-data',
            connector_path='etl/inegi_intercensal_census/conns.yaml'
        )

        read = ReadStep()
        clean = CleanStep()
        transform = TransformStep()
        income_transform = IncomeIntervalStep()

        load_step = LoadStep(
            "inegi_housing", db_connector, if_exists="append", pk=['sex', 'mun_id'], dtype=dtype
        )

        return [download_step, read, clean, transform, income_transform, load_step]