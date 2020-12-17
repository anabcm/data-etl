import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import DownloadStep
from bamboo_lib.steps import LoadStep
from shared import get_dimensions, COUNTRY_REPLACE, INVESTMENT_TYPE
from helpers import norm


class Transform_101_Step(PipelineStep):
    def run_step(self, prev, params):
        data = prev

        df = pd.read_excel(data, sheet_name='10.1')
        df.columns = [norm(x.strip().lower().replace(' ', '_').replace('-', '_').replace('%', 'perc')) for x in df.columns]
        df.columns = ['year', 'country', 'value', 'count', 'value_c']
        df = df.loc[~df['year'].astype(str).str.contains('Total')].copy()
        df = df.loc[df['value_c'] != 'C'].copy()
        df.drop(columns=['value'], inplace=True)

        dim_country = get_dimensions()[1]

        df['country'].replace(COUNTRY_REPLACE, inplace=True)

        df['country'] = df['country'].replace(dict(zip(dim_country['country_name_es'], dim_country['iso3'])))

        df[['year', 'count', 'value_c']] = df[['year', 'count', 'value_c']].astype(float)

        return df

class Transform_102_Step(PipelineStep):
    def run_step(self, prev, params):
        data = prev

        df = pd.read_excel(data, sheet_name='10.2')
        df.columns = [norm(x.strip().lower().replace(' ', '_').replace('-', '_').replace('%', 'perc')) for x in df.columns]
        df.columns = ['year', 'country', 'investment_type', 'value', 'count', 'value_c']
        df = df.loc[~df['year'].astype(str).str.contains('Total')].copy()
        df = df.loc[df['value_c'] != 'C'].copy()
        df.drop(columns=['value'], inplace=True)

        dim_country = get_dimensions()[1]

        df['country'].replace(COUNTRY_REPLACE, inplace=True)

        df['country'] = df['country'].replace(dict(zip(dim_country['country_name_es'], dim_country['iso3'])))

        df[['year', 'count', 'value_c']] = df[['year', 'count', 'value_c']].astype(float)

        df['investment_type'].replace(INVESTMENT_TYPE, inplace=True)

        return df

class Transform_103_Step(PipelineStep):
    def run_step(self, prev, params):
        data = prev

        df = pd.read_excel(data, sheet_name='10.3')
        df.columns = [norm(x.strip().lower().replace(' ', '_').replace('-', '_').replace('%', 'perc')) for x in df.columns]
        df.columns = ['year', 'value_between_companies', 'value_new_investments', 'value_re_investments', 
                    'count_between_companies', 'count_new_investments', 'count_re_investments',
                    'value_between_companies_c', 'value_new_investments_c', 'value_re_investments_c']
        df = df.loc[~df['year'].astype(str).str.contains('Total')].copy()

        df.drop(columns=['value_between_companies', 'value_new_investments', 'value_re_investments'], inplace=True)

        df[list(df.columns)] = df[list(df.columns)].astype(float)

        base = ['year']
        df_final = pd.DataFrame()
        for option in ['between_companies', 'new_investments', 're_investments']:
            temp = df[base + ['count_{}'.format(option), 'value_{}_c'.format(option)]]
            temp.columns = ['year', 'count', 'value_c']
            temp.dropna(subset=['value_c'], inplace=True)
            temp['investment_type'] = option
            df_final = df_final.append(temp)
        df = df_final.copy()

        df['investment_type'].replace(INVESTMENT_TYPE, inplace=True)

        return df

class FDI10Pipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return [
            Parameter(name='table', dtype=float)
        ]

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))

        download_step = DownloadStep(
            connector='fdi-data',
            connector_path='conns.yaml',
            force=True
        )

        transform_101_step = Transform_101_Step()
        transform_102_step = Transform_102_Step()
        transform_103_step = Transform_103_Step()

        if params.get('table') == 10.1:

            load_step = LoadStep('fdi_10_year_country', db_connector, if_exists='drop', 
                    pk=['country', 'year'], dtype={'year':    'UInt16',
                                                   'count':   'UInt16',
                                                   'value_c': 'Float32'})

            return [download_step, transform_101_step, load_step]

        if params.get('table') == 10.2:

            load_step = LoadStep('fdi_10_year_country_investment', db_connector, if_exists='drop', 
                    pk=['country', 'year'], dtype={'year':             'UInt16',
                                                   'investment_type':  'UInt8',
                                                   'count':            'UInt16',
                                                   'value_c':          'Float32'})

            return [download_step, transform_102_step, load_step]

        if params.get('table') == 10.3:

            load_step = LoadStep('fdi_10_year_investment', db_connector, if_exists='drop', 
                    pk=['year'], dtype={'year':             'UInt16',
                                        'investment_type':  'UInt8',
                                        'count':            'UInt16',
                                        'value_c':          'Float32'})

            return [download_step, transform_103_step, load_step]

if __name__ == '__main__':
    pp = FDI10Pipeline()
    for i in [10.1, 10.2, 10.3]:
        pp.run({'table': i})