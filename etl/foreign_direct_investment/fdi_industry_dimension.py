import nltk
import pandas as pd
from bamboo_lib.models import PipelineStep, EasyPipeline, Parameter
from bamboo_lib.connectors.models import Connector
from bamboo_lib.steps import DownloadStep, LoadStep
from sklearn.feature_extraction import stop_words
from util import format_text

class ReadStep(PipelineStep):
    def run_step(self, prev, params):

        df = pd.read_csv(prev)

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

        # codes ids
        cols_es = list(df.columns[df.columns.str.contains('_es')])
        cols_en = list(df.columns[df.columns.str.contains('_en')])
        nltk.download('stopwords')
        stopwords_es = nltk.corpus.stopwords.words('spanish')
        df = format_text(df, cols_es, stopwords=stopwords_es)
        df = format_text(df, cols_en, stopwords=stop_words.ENGLISH_STOP_WORDS)

        df[['subsector_id', 'industry_group_id']] = df[['subsector_id', 'industry_group_id']].astype(int)

        return df

class FDIIndustryPipeline(EasyPipeline):
    @staticmethod	
    def parameter_list():	
        return [	
            Parameter(name='table', dtype=float)	
        ]

    @staticmethod
    def steps(params, **kwargs):
        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))

        dtypes = {
            'sector_id':           'String',
            'subsector_id':        'UInt16',
            'industry_group_id':   'UInt16'
        }

        download_step = DownloadStep(
            connector='fdi-scian',
            connector_path='conns.yaml',
            force=True
        )

        read_step = ReadStep()
        load_step = LoadStep(params.get('table'), db_connector, dtype=dtypes,
                if_exists='drop', pk=['sector_id', 'subsector_id', 'industry_group_id'])
        
        return [download_step, read_step, load_step]

if __name__ == '__main__':
    pp = FDIIndustryPipeline()
    for table in ['dim_shared_industry_fdi_legacy', 'dim_shared_industry_fdi']:
        pp.run({
            'table': table
        })