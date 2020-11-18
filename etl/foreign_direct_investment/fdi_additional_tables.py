import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import PipelineStep, EasyPipeline, Parameter
from bamboo_lib.steps import DownloadStep, LoadStep
from helpers import norm
from shared import get_dimensions, SECTOR_REPLACE, COUNTRY_REPLACE, INVESTMENT_TYPE
from util import validate_category

class ReadStep(PipelineStep):
    def run_step(self, prev, params):
        print(params)
        df = pd.read_excel(prev, sheet_name=params.get('sheet_name'))
        df.rename(columns={
            'Año': 'year',
            'Trimestre': 'quarter_id',
            'Tipo de inversión': 'investment_type',
            'Sector': 'sector_id',
            'Subsector': 'subsector_id',
            'Rama': 'industry_group_id',
            'País de Origen DEAE': 'country_id',
            'Entidad federativa': 'ent_id',
            'Monto': 'value',
            'Recuento': 'count',
            'Monto C': 'value_c'
        }, inplace=True)

        return df

class TransformInvestmentStep(PipelineStep):
    def run_step(self, prev, params):
        df = prev
        print('Investmen STEP')
        pk_id = params.get('pk')

        df = df.loc[df[pk_id] != 'Total general'].copy()

        df['quarter_id'] = (df['year'].astype(int).astype(str) + df['quarter_id'].astype(int).astype(str)).astype(int)
        df.drop(columns=['year'], inplace=True)

        split = df[pk_id].str.split(' ', n=1, expand=True)
        df[pk_id] = split[0]
        df[pk_id] = df[pk_id].astype(int)

        level = ['sector_id', 'subsector_id', 'industry_group_id']
        for i in level:
            if i != pk_id:
                df[i] = 0

        df['sector_id'].replace(SECTOR_REPLACE, inplace=True)
        df['sector_id'] = df['sector_id'].astype(str)

        df['value_c'] = df['value_c'].astype(str).str.lower()

        temp = pd.DataFrame()
        for investment_type in list(df['investment_type'].unique()):
            temp = temp.append(validate_category(df.loc[(df['investment_type'] == investment_type)], pk_id, 'value_c', 'c'))

        df = temp.copy()
        temp = pd.DataFrame()

        df = df.loc[df['value_c'] != 'c'].copy()

        df['investment_type'].replace(INVESTMENT_TYPE, inplace=True)

        df[['value', 'count', 'investment_type', 'quarter_id', 'industry_group_id', 'subsector_id']] = \
            df[['value', 'count', 'investment_type', 'quarter_id', 'industry_group_id', 'subsector_id']].astype(float)

        return df

class TransformCountryStep(PipelineStep):
    def run_step(self, prev, params):
        df = prev
        print('Country STEP')
        pk_id = [x for x in df.columns if ('id' in x) & ('country' not in x)][0]

        df = df.loc[~df[pk_id].isna()].copy()

        split = df[pk_id].str.split(' ', n=1, expand=True)
        df[pk_id] = split[0]
        df[pk_id] = df[pk_id].astype(int)

        level = ['sector_id', 'subsector_id', 'industry_group_id']
        for i in level:
            if i != pk_id:
                df[i] = 0

        df['sector_id'].replace(SECTOR_REPLACE, inplace=True)
        df['sector_id'] = df['sector_id'].astype(str)

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

        df[['year', 'value', 'count', 'industry_group_id', 'subsector_id']] = \
            df[['year', 'value', 'count', 'industry_group_id', 'subsector_id']].astype(float)

        return df

class TransformStateStep(PipelineStep):
    def run_step(self, prev, params):
        df = prev
        print('State STEP')
        pk_id = [x for x in df.columns if ('id' in x) & ('country' not in x) & ('ent_id' not in x)][0]

        df = df.loc[~df[pk_id].isna()].copy()

        split = df[pk_id].str.split(' ', n=1, expand=True)
        df[pk_id] = split[0]
        df[pk_id] = df[pk_id].astype(int)

        df['value_c'] = df['value_c'].astype(str).str.lower()

        temp = pd.DataFrame()
        for country in list(df['ent_id'].unique()):
            temp = temp.append(validate_category(df.loc[(df['ent_id'] == country)], pk_id, 'value_c', 'c'))

        df = temp.copy()
        temp = pd.DataFrame()

        ent_replace = get_dimensions()[0]

        df['ent_id'].replace(dict(zip(ent_replace['ent_name'], ent_replace['ent_id'])), inplace=True)

        df = df.loc[df['value_c'] != 'c'].copy()

        level = ['sector_id', 'subsector_id', 'industry_group_id']
        for i in level:
            if i != pk_id:
                df[i] = 0

        df['sector_id'].replace(SECTOR_REPLACE, inplace=True)
        df['sector_id'] = df['sector_id'].astype(str)

        df = df.loc[df['value_c'] != 'c'].copy()

        df[['year', 'value_c', 'count', 'industry_group_id', 'subsector_id']] = \
            df[['year', 'value_c', 'count', 'industry_group_id', 'subsector_id']].astype(float)

        return df

class TransformYearStep(PipelineStep):
    def run_step(self, prev, params):
        df = prev
        print('Year STEP')
        pk_id = [x for x in df.columns if ('id' in x) & ('country' not in x) & ('ent_id' not in x)][0]

        df = df.loc[~df[pk_id].isna()].copy()

        split = df[pk_id].str.split(' ', n=1, expand=True)
        df[pk_id] = split[0]
        df[pk_id] = df[pk_id].astype(int)

        df['value_c'] = df['value_c'].astype(str).str.lower()

        temp = pd.DataFrame()
        for country in list(df['ent_id'].unique()):
            temp = temp.append(validate_category(df.loc[(df['ent_id'] == country)], pk_id, 'value_c', 'c'))

        df = temp.copy()
        temp = pd.DataFrame()

        level = ['sector_id', 'subsector_id', 'industry_group_id']
        for i in level:
            if i != pk_id:
                df[i] = 0

        df['sector_id'].replace(SECTOR_REPLACE, inplace=True)
        df['sector_id'] = df['sector_id'].astype(str)

        df = df.loc[df['value_c'] != 'c'].copy()

        df = df.loc[df['value_c'] != 'false'].copy()

        df[['year', 'value_c', 'count', 'industry_group_id', 'subsector_id']] = \
            df[['year', 'value_c', 'count', 'industry_group_id', 'subsector_id']].astype(float)

        return df

class TransformYearQuarterStep(PipelineStep):
    def run_step(self, prev, params):
        df = prev
        print('Year YearQuarter')
        pk_id = [x for x in df.columns if ('id' in x) & ('country' not in x) & ('ent_id' not in x)][0]

        df = df.loc[df[pk_id] != 'Total general'].copy()

        df['quarter_id'] = (df['year'].astype(int).astype(str) + df['quarter_id'].astype(int).astype(str)).astype(int)
        df.drop(columns=['year'], inplace=True)

        split = df[pk_id].str.split(' ', n=1, expand=True)
        df[pk_id] = split[0]
        df[pk_id] = df[pk_id].astype(int)

        df['value_c'] = df['value_c'].astype(str).str.lower()

        temp = pd.DataFrame()
        for country in list(df['ent_id'].unique()):
            temp = temp.append(validate_category(df.loc[(df['ent_id'] == country)], pk_id, 'value_c', 'c'))

        df = temp.copy()
        temp = pd.DataFrame()
        #df = validate_category(df, pk_id, 'value_c', 'c')

        level = ['sector_id', 'subsector_id', 'industry_group_id']
        for i in level:
            if i != pk_id:
                df[i] = 0

        df['sector_id'].replace(SECTOR_REPLACE, inplace=True)
        df['sector_id'] = df['sector_id'].astype(str)

        df = df.loc[df['value_c'] != 'c'].copy()

        df = df.loc[df['value_c'] != 'false'].copy()

        df[['quarter_id', 'value_c', 'count', 'industry_group_id', 'subsector_id']] = \
            df[['quarter_id', 'value_c', 'count', 'industry_group_id', 'subsector_id']].astype(float)

        return df

class TransformYearInvestmentStep(PipelineStep):
    def run_step(self, prev, params):
        df = prev
        print('Year YearInvestment')
        pk_id = [x for x in df.columns if ('id' in x) & ('country' not in x) & ('ent_id' not in x)][0]

        df = df.loc[df[pk_id] != 'Total general'].copy()

        split = df[pk_id].str.split(' ', n=1, expand=True)
        df[pk_id] = split[0]
        df[pk_id] = df[pk_id].astype(int)

        df['value_c'] = df['value_c'].astype(str).str.lower()

        temp = pd.DataFrame()
        for country in list(df['ent_id'].unique()):
            temp = temp.append(validate_category(df.loc[(df['ent_id'] == country)], pk_id, 'value_c', 'c'))

        df = temp.copy()
        temp = pd.DataFrame()

        level = ['sector_id', 'subsector_id', 'industry_group_id']
        for i in level:
            if i != pk_id:
                df[i] = 0

        df['sector_id'].replace(SECTOR_REPLACE, inplace=True)
        df['sector_id'] = df['sector_id'].astype(str)

        df = df.loc[df['value_c'] != 'c'].copy()

        df.drop(columns=['value_c'], inplace=True)

        df['investment_type'].replace(INVESTMENT_TYPE, inplace=True)

        df[['year', 'value', 'count', 'investment_type', 'industry_group_id', 'subsector_id']] = \
            df[['year', 'value', 'count', 'investment_type', 'industry_group_id', 'subsector_id']].astype(float)

        return df

class Pipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return [
            Parameter(name="pk", dtype=str),
            Parameter(name="sheet_name", dtype=str),
            Parameter(name="dtype", dtype=str),
            Parameter(name="table", dtype=str),
            Parameter(name="db-source", dtype=str),
            Parameter(name='if_exists', dtype=str)
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
            params.get('table'), db_connector, if_exists=params.get('if_exists'), 
            pk=[params.get('pk')], dtype=params.get('dtype')
        )

        if params.get('db-source') == 'fdi-data-additional-2':
            transform_step = TransformInvestmentStep()
        
        elif params.get('db-source') == 'fdi-data-additional':
            if int(params.get('sheet_name')) in range(4, 7):
                transform_step = TransformCountryStep()
            else:
                transform_step = TransformStateStep()

        else:
            if int(params.get('sheet_name')) in range(1, 4):
                transform_step = TransformYearStep()
            elif int(params.get('sheet_name')) in range(4, 7):
                transform_step = TransformYearQuarterStep()
            else:
                transform_step = TransformYearInvestmentStep()

        return [download_step, read_step, transform_step, load_step]