import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, PipelineStep, Parameter
from bamboo_lib.steps import LoadStep

class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        # read data
        url = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vRTAtN97MAri4ZgYyYQcWR_OO8iFbfopAwQhCtdqfb1yxnvo0y_yVc4qLCA0Z-0heKzX-7nWUuT24FV/pub?output=xlsx'
        df = pd.read_excel(url, sheet_name='Country Groupings')

        df.columns = df.columns.str.lower()

        df = df[['id', 'country', 'continent', 'oecd']].copy()

        df.rename(columns={'continent': 'continent_id',
                           'country': 'country_name',
                           'id': 'iso3'}, inplace=True)

        df['continent'] = df['continent_id']

        continents = {
            'af': 'Africa',
            'na': 'North America',
            'oc': 'Oceania',
            'an': 'Antarctica',
            'as': 'Asia',
            'eu': 'Europe',
            'sa': 'South America'
        }
        df['continent'].replace(continents, inplace=True)

        return df

class CountryPipeline(EasyPipeline):
    @staticmethod
    def steps(params):
        
        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))

        dtype = {
            'iso3': 'String', 
            'country_name': 'String', 
            'continent_id': 'String',
            'continent': 'String',
            'oecd': 'UInt8'
        }
        
        transform_step = TransformStep()
        load_step = LoadStep('dim_shared_country', db_connector, if_exists='drop', pk=[ 'iso3'], dtype=dtype)

        return [transform_step, load_step]