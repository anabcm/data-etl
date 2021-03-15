
import nltk
import pandas as pd
from bamboo_lib.models import PipelineStep
from bamboo_lib.models import EasyPipeline
from bamboo_lib.connectors.models import Connector
from bamboo_lib.steps import LoadStep
from bamboo_lib.helpers import query_to_df
from sklearn.feature_extraction import stop_words
from util import format_text

class ReadStep(PipelineStep):
    def run_step(self, prev, params):

        df = pd.read_csv('https://docs.google.com/spreadsheets/d/e/2PACX-1vRf6ecVlEDTaBNfp2VSd7Ti-AnAQDyQlMjF7uek-cQHQ49ihWv4zeSXgN8z0gJV72ogir3hYvYTu8iX/pub?output=csv')

        for locale in ['es', 'en']:
            for level in ['sector', 'subsector', 'industry_group', 'naics_industry', 'national_industry']:
                df.sort_values(by=['{}_id'.format(level)], inplace=True)
                df['{}_{}_short'.format(level, locale)] = df['{}_{}_short'.format(level, locale)].ffill()
                df['{}_{}'.format(level, locale)] = df['{}_{}'.format(level, locale)].ffill()
                df.loc[df['{}_{}_short'.format(level, locale)].isna(), '{}_{}_short'.format(level, locale)] = \
                    df.loc[df['{}_{}_short'.format(level, locale)].isna(), '{}_{}'.format(level, locale)]

        # codes ids
        cols_es = list(df.columns[df.columns.str.contains('_es')])
        cols_en = list(df.columns[df.columns.str.contains('_en')])
        nltk.download('stopwords')
        stopwords_es = nltk.corpus.stopwords.words('spanish')
        df = format_text(df, cols_es, stopwords=stopwords_es)
        df = format_text(df, cols_en, stopwords=stop_words.ENGLISH_STOP_WORDS)

        for col in ['sector_id', 'subsector_id', 'industry_group_id', 'naics_industry_id', 'national_industry_id']:
            df[col] = df[col].astype(str)

        # when creating the industry dimension, the cms ask for members, so 'ghost' profiles are created
        # they also appear at the search bar which the canon-cms-warmup also gets
        query = 'SELECT distinct(national_industry_id) FROM inegi_economic_census'
        query_result = list(query_result['national_industry_id'])
        df = df.loc[df['national_industry_id'].isin(query_result)].copy()

        return df

class IndustryPipeline(EasyPipeline):
    @staticmethod
    def steps(params, **kwargs):
        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))

        dtypes = {
            'sector_id':                      'String',
            'subsector_id':                   'String',
            'industry_group_id':              'String',
            'naics_industry_id':              'String',
            'national_industry_id':           'String'
        }

        read_step = ReadStep(connector=db_connector)
        load_step = LoadStep('dim_shared_industry_economic_census', db_connector, dtype=dtypes,
                if_exists='drop', pk=['sector_id', 'subsector_id', 'industry_group_id', 'naics_industry_id', 'national_industry_id'])
        
        return [read_step, load_step]

if __name__ == '__main__':
    pp = IndustryPipeline()
    pp.run({})
