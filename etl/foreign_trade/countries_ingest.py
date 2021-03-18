import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, PipelineStep, Parameter
from bamboo_lib.steps import LoadStep, DownloadStep

from helpers import COUNTRIES

class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        # read data
        # translations
        df = pd.read_excel(prev, sheet_name='translations')

        df = df.loc[(df.lang == 'es'), ['origin_id', 'lang', 'name']].copy()
        translations = df.copy()

        # countries
        df = pd.read_excel(prev, sheet_name='Country Groupings')
        countries = pd.read_excel(prev, sheet_name='countries')

        df.columns = df.columns.str.lower()

        df = df[['id', 'country', 'continent', 'oecd']].copy()

        df.rename(columns={'continent': 'continent_id',
                        'country': 'country_name',
                        'id': 'iso3'}, inplace=True)

        df['continent'] = df['continent_id']
        df['continent_es'] = df['continent_id']
        df['iso2'] = df['iso3']
        df['id_num'] = df['iso3']

        continents = {
            'af': 'Africa',
            'na': 'North America',
            'oc': 'Oceania',
            'an': 'Antarctica',
            'as': 'Asia',
            'eu': 'Europe',
            'sa': 'South America'
        }

        continents_es = {
            'af': 'África',
            'na': 'América del Norte',
            'oc': 'Oceanía',
            'an': 'Antártida',
            'as': 'Asia',
            'eu': 'Europa',
            'sa': 'América del Sur'
        }

        df['continent'].replace(continents, inplace=True)
        df['continent_es'].replace(continents_es, inplace=True)
        df['iso2'].replace(dict(zip(countries['id_3char'], countries['id_2char'])), inplace=True)
        df['id_num'].replace(dict(zip(countries['id_3char'], countries['id_num'])), inplace=True)
        df['id_num'] = df['id_num'].astype(str)

        # name es
        df['country_name_es'] = df['continent_id'] + df['iso3']
        df['country_name_es'].replace(dict(zip(translations['origin_id'], translations['name'])), inplace=True)

        df = df.append(COUNTRIES, ignore_index=True)

        return df

class CountryPipeline(EasyPipeline):
    @staticmethod
    def steps(params):
        
        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))

        dtype = {
            'iso2':            'String',
            'iso3':            'String', 
            'country_name':    'String',
            'country_name_es': 'String',
            'continent_id':    'String',
            'continent':       'String',
            'continent_es':    'String',
            'oecd':            'UInt8'
        }

        download_step = DownloadStep(
            connector='countries',
            connector_path='conns.yaml'
        )

        transform_step = TransformStep()
        load_step = LoadStep('dim_shared_country', db_connector, if_exists='drop', pk=['iso3', 'continent_id'], 
                            dtype=dtype, engine='ReplacingMergeTree', 
                            nullable_list=['iso2', 'id_num', 'continent', 'continent_es'])

        return [download_step, transform_step, load_step]

if __name__ == '__main__':
    pp = CountryPipeline()
    pp.run({})