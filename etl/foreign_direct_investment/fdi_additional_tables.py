import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import DownloadStep
from bamboo_lib.steps import LoadStep
from helpers import norm
from shared import get_dimensions, SECTOR_REPLACE, COUNTRY_REPLACE, INVESTMENT_TYPE
from util import validate_category

class ReadStep(PipelineStep):
    def run_step(self, prev, params):
        df = pd.read_excel(prev, sheet_name=params.get('sheet_name'))
        df.rename(columns={
            'Año': 'year',
            'Trimestre': 'quarter_id',
            'Tipo de inversión': 'investment_type',
            'Sector': 'sector_id',
            'Subsector': 'subsector_id',
            'Rama': 'industry_group_id',
            'País de Origen DEAE': 'country_id',
            'Monto': 'value',
            'Recuento': 'count',
            'Monto C': 'value_c'
        }, inplace=True)

        return df

class TransformInvestmentStep(PipelineStep):
    def run_step(self, prev, params):
        df = prev

        pk_id = params.get('pk')

        df = df.loc[df[pk_id] != 'Total general'].copy()

        df['quarter_id'] = (df['year'].astype(int).astype(str) + df['quarter_id'].astype(int).astype(str)).astype(int)
        df.drop(columns=['year'], inplace=True)

        split = df[pk_id].str.split(' ', n=1, expand=True)
        df[pk_id] = split[0]
        df[pk_id] = df[pk_id].astype(int)

        if params.get('pk') == 'sector_id':
            df[pk_id].replace(SECTOR_REPLACE, inplace=True)
            df[pk_id] = df[pk_id].astype(str)

        df['value_c'] = df['value_c'].astype(str).str.lower()

        temp = pd.DataFrame()
        for investment_type in list(df['investment_type'].unique()):
            temp = temp.append(validate_category(df.loc[(df['investment_type'] == investment_type)], pk_id, 'value_c', 'c'))

        df = temp.copy()
        temp = pd.DataFrame()

        df = df.loc[df['value_c'] != 'c'].copy()

        df['investment_type'].replace(INVESTMENT_TYPE, inplace=True)

        df[['value', 'count', 'investment_type']] = df[['value', 'count', 'investment_type']].astype(float)

        return df

class TransformCountryStep(PipelineStep):
    def run_step(self, prev, params):
        df = prev

        pk_id = [x for x in df.columns if ('id' in x) & ('country' not in x)][0]

        df = df.loc[~df[pk_id].isna()].copy()

        split = df[pk_id].str.split(' ', n=1, expand=True)
        df[pk_id] = split[0]
        df[pk_id] = df[pk_id].astype(int)

        if params.get('pk') == 'sector_id':
            df[pk_id].replace(SECTOR_REPLACE, inplace=True)
            df[pk_id] = df[pk_id].astype(str)

        df['value_c'] = df['value_c'].astype(str).str.lower()

        temp = pd.DataFrame()
        for country in list(df['country_id'].unique()):
            temp = temp.append(validate_category(df.loc[(df['country_id'] == country)], pk_id, 'value_c', 'c'))

        df = temp.copy()
        temp = pd.DataFrame()

        country_replace = get_dimensions()[1]

        df['country_id'].replace(COUNTRY_REPLACE, inplace=True)

        df['country_id'].replace(dict(zip(country_replace['country_name_es'], country_replace['iso3'])), inplace=True)

        df = df.loc[df['value_c'] != 'c'].copy()

        df[['year', 'value', 'count']] = df[['year', 'value', 'count']].astype(float)

        return df

class Pipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return [
            Parameter(name="pk", dtype=str),
            Parameter(name="sheet_name", dtype=str),
            Parameter(name="dtype", dtype=str),
            Parameter(name="table", dtype=str),
            Parameter(name="db-source", dtype=str)
        ]

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch("clickhouse-database", open("../conns.yaml"))

        download_step = DownloadStep(
            connector=params.get('db-source'),
            connector_path="conns.yaml"
        )

        read_step = ReadStep()
        
        load_step = LoadStep(
            params.get('table'), db_connector, if_exists="drop", 
            pk=[params.get('pk')], dtype=params.get('dtype')
        )

        if int(params.get('sheet_name')) < 4:
            transform_step = TransformInvestmentStep()
            return [download_step, read_step, transform_step, load_step]
        
        else:
            transform_step = TransformCountryStep()
            return [download_step, read_step, transform_step, load_step]