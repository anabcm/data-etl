
import nltk
import pandas as pd
from bamboo_lib.models import PipelineStep
from bamboo_lib.models import EasyPipeline
from bamboo_lib.connectors.models import Connector
from bamboo_lib.steps import LoadStep
from sklearn.feature_extraction import stop_words
from util import format_text

class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        df = pd.read_csv(prev[0])

        for locale in ['es', 'en']:
            for level in ['sector', 'subsector', 'industry_group']:
                df.sort_values(by=['{}_id'.format(level)], inplace=True)
                df['{}_{}_short'.format(level, locale)] = df['{}_{}_short'.format(level, locale)].ffill()
                df['{}_{}'.format(level, locale)] = df['{}_{}'.format(level, locale)].ffill()
                df.loc[df['{}_{}_short'.format(level, locale)].isna(), '{}_{}_short'.format(level, locale)] = \
                    df.loc[df['{}_{}_short'.format(level, locale)].isna(), '{}_{}'.format(level, locale)]

        for col in ['sector_id', 'subsector_id', 'industry_group_id']:
            df[col] = df[col].astype(str)

        df = df[['sector_id', 'sector_es', 'sector_es_short', 'sector_en',
                 'sector_en_short', 'subsector_id', 'subsector_es', 'subsector_es_short',
                 'subsector_en', 'subsector_en_short', 'industry_group_id',
                 'industry_group_es', 'industry_group_es_short', 'industry_group_en',
                 'industry_group_en_short']].copy()

        df.drop_duplicates(subset=['industry_group_id'], inplace=True)

        MISSING_DIMENSION = pd.read_csv(prev[1])

        df = df.append(MISSING_DIMENSION)

        for col in df.columns:
            df[col] = df[col].astype(str)

        # codes ids
        cols_es = list(df.columns[df.columns.str.contains('_es')])
        cols_en = list(df.columns[df.columns.str.contains('_en')])
        nltk.download('stopwords')
        stopwords_es = nltk.corpus.stopwords.words('spanish')
        df = format_text(df, cols_es, stopwords=stopwords_es)
        df = format_text(df, cols_en, stopwords=stop_words.ENGLISH_STOP_WORDS)

        return df

class ENOEIndustryPipeline(EasyPipeline):
    @staticmethod
    def steps(params, **kwargs):
        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))
        download_step = DownloadStep(
            connector=['naics-scian-codes', 'missing-dim'],
            connector_path="conns.yaml"
        )
        transform_step = TransformStep()
        load_step = LoadStep('dim_shared_industry_enoe', db_connector, if_exists='drop', pk=['industry_group_id', 'subsector_id', 'sector_id'])
        
        return [download_step, transform_step, load_step]
