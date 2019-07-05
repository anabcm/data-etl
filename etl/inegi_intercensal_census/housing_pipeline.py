
import pandas as pd
from bamboo_lib.models import PipelineStep, AdvancedPipelineExecutor
from bamboo_lib.models import Parameter, EasyPipeline
from bamboo_lib.connectors.models import Connector
from bamboo_lib.steps import DownloadStep
from bamboo_lib.steps import LoadStep
from bamboo_lib.helpers import grab_connector

class ReadStep(PipelineStep):
    def run_step(self, prev, params):
        # intercensal census data
        df = pd.read_csv(prev, encoding='latin-1', dtype={'ENT': 'str', 'MUN': 'str', 'LOC50K': 'str'})
        # data to replace
        url = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vR08Js9Sh4nNTMe5uBcsDUFedG5MOjIf90p6EHAr1_sWY5kpnI3xUvyPHzQpTEUrXz1pskaoc0uyea6/pub?output=xlsx'
        data = {}
        for col in ['pisos', 'techos', 'paredes', 'cobertura', 'financiamiento', 'clavivp', 'totcuart', 'cuadorm', 'ingr_ayugob', 'ingr_perotropais', 'deuda', 'forma_adqui']:
            data[col] = pd.read_excel(url, sheet_name=col, encoding='latin-1', dtype='object')
        return df, data

class CleanStep(PipelineStep):
    def run_step(self, prev, params):
        df, data = prev[0], prev[1]
        # preformat
        df.columns = df.columns.str.lower()
        # location level
        df['loc_id'] = (df.ent.astype('str') + df.mun.astype('str') + df.loc50k.astype('str')).astype('int')
        # column type conversion
        df['ingtrhog'] = df['ingtrhog'].astype('float')
        df.ingtrhog = df.ingtrhog.fillna(0).round(0).astype('int64')
        return df, data

class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        df, data = prev[0], prev[1]
        # replace, select and group data
        for col in data.keys():
            df[col] = df[col].replace(dict(zip(data[col]['prev_id'], data[col]['id'].astype('int'))))
        df = df[['loc_id', 'cobertura', 'ingtrhog', 'pisos', 'techos', 'paredes', 'forma_adqui', 'deuda', 'factor', 'numpers', 'financiamiento', 'totcuart', 'cuadorm', 'clavivp', 'ingr_ayugob', 'ingr_perotropais']].copy(deep=True)
        df.fillna('temp', inplace=True)
        df = df.groupby(['loc_id', 'factor', 'cobertura', 'clavivp', 'forma_adqui', 'financiamiento', 'deuda', 'ingr_ayugob', 'ingr_perotropais', 'ingtrhog', 'pisos', 'techos', 'paredes', 'numpers', 'totcuart', 'cuadorm']).sum().reset_index(col_fill='ffill')
        df = df.rename(columns={'factor': 'inhabitants', 'pisos': 'floor', 'paredes': 'wall', 'techos': 'roof', 'forma_adqui': 'acquisition', 'deuda': 'debt', 'ingtrhog': 'income', 'cobertura': 'coverage', 'clavivp': 'home_type', 'financiamiento': 'funding', 'ingr_ayugob': 'government_financial_aid', 'ingr_perotropais': 'foreign_financial_aid', 'numpers': 'n_inhabitants', 'totcuart': 'total_rooms', 'cuadorm': 'bedrooms'})
        df.replace('temp', pd.np.nan, inplace=True)
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
        for col in df.columns:
            df[col] = df[col].astype('object')
        return df

class CoveragePipeline(EasyPipeline):
    @staticmethod
    def pipeline_id():
        return 'program-coverage-pipeline-temp'

    @staticmethod
    def name():
        return 'Program Coverage Pipeline temp'

    @staticmethod
    def description():
        return 'Processes information from Intercensal Census 2015, Mexico'

    @staticmethod
    def website():
        return 'http://datawheel.us'

    @staticmethod
    def parameter_list():
        return [
            Parameter(label='Index', name='index', dtype=str),
            Parameter(label='Source connector', name='source-connector', dtype=str, source=Connector)
        ]

    @staticmethod
    def steps(params):
        # Use of connectors specified in the conns.yaml file
        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))
        dtype = {
            'loc_id':                   'UInt32',
            'inhabitants':              'UInt16',
            'floor':                    'UInt8',
            'wall':                     'UInt8',
            'roof':                     'UInt8',
            'acquisition':              'UInt8',
            'debt':                     'UInt8',
            'income':                   'UInt8',
            'coverage':                 'UInt8',
            'home_type':                'UInt8',
            'funding':                  'UInt8',
            'government_financial_aid': 'UInt8',
            'foreign_financial_aid':    'UInt8',
            'n_inhabitants':            'UInt8',
            'total_rooms':              'UInt8',
            'bedrooms':                 'UInt8'
        }

        download_step = DownloadStep(
            connector='housing-data',
            connector_path='conns.yaml'
        )

        read_step = ReadStep()
        clean_step = CleanStep()
        transform_step = TransformStep()
        income_transform_step = IncomeIntervalStep()

        load_step = LoadStep(
            'inegi_housing', db_connector, if_exists='append', pk=['loc_id'], dtype=dtype, 
            nullable_list=['inhabitants', 'floor', 'wall', 'roof', 'acquisition', 'debt', 'income', 'coverage',
                          'home_type', 'funding', 'government_financial_aid', 'foreign_financial_aid',
                          'n_inhabitants', 'total_rooms', 'bedrooms']
        )

        return [download_step, read_step, clean_step, transform_step, income_transform_step, load_step]