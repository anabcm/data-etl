
import nltk
import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, PipelineStep, Parameter
from bamboo_lib.steps import LoadStep, DownloadStep
from sklearn.feature_extraction import stop_words
from helpers import format_text, fill_values

class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        # read data
        hs2 = pd.read_excel(prev, sheet_name='hs2')
        hs4 = pd.read_excel(prev, sheet_name='hs4')
        hs6 = pd.read_excel(prev, sheet_name='hs6')
        chapter = pd.read_excel(prev, sheet_name='chapter')

        ran = {'hs2': 4, 
               'hs4': 6, 
               'chapter': 2}
        langs = ['es', 'en']

        for k, v in ran.items():
            ids = hs6['id'].astype('str').str.zfill(8).str[:v].astype('int')
            hs6['{}_id'.format(k)] = ids
            for lan in langs:
                hs6['{}_{}'.format(k, lan)] = ids
                hs6['{}_{}_short'.format(k, lan)] = ids

        ran = {'hs2': hs2, 
               'hs4': hs4, 
               'hs6': hs6, 
               'chapter': chapter}
        for k, v in ran.items():
            for lan in langs:
                target = '{}_{}_short'.format(k, lan)
                base = '{}_{}'.format(k, lan)
                v = fill_values(v, target, base)

        for k, v in ran.items():
            for lan in langs:
                target = '{}_{}_short'.format(k, lan)
                base = '{}_{}'.format(k, lan)
                hs6[target].replace(dict(zip(v['id'], v[target])), inplace=True)
                hs6[base].replace(dict(zip(v['id'], v[base])), inplace=True)

        hs6.drop(columns=['trade_value'], inplace=True)

        hs6.rename(columns={'id': 'hs6_id',
                            'chapter_id': 'chapter'}, inplace=True)

        hs6.sort_values(by='hs6_id', inplace=True)

        cols_es = ['chapter_es', 'chapter_es_short', 'hs2_es', 'hs2_es_short', 'hs4_es', 'hs4_es_short', 'hs6_es', 'hs6_es_short']
        cols_en = ['chapter_en', 'chapter_en_short', 'hs2_en', 'hs2_en_short', 'hs4_en', 'hs4_en_short', 'hs6_en', 'hs6_en_short']

        # codes ids
        nltk.download('stopwords')
        stopwords_es = nltk.corpus.stopwords.words('spanish')
        hs6 = format_text(hs6, cols_es, stopwords=stopwords_es)
        hs6 = format_text(hs6, cols_en, stopwords=stop_words.ENGLISH_STOP_WORDS)

        for col in ['hs6_id', 'hs4_id', 'hs2_id', 'chapter']:
            hs6[col] = hs6[col].astype('int')

        cols = ['chapter', 'chapter_es', 'chapter_en', 'hs2_id', 'hs2_es', 
            'hs2_en', 'hs2_es_short', 'hs2_en_short']

        df = hs6[cols].copy()
        df = df.groupby(cols).sum().reset_index(col_fill='ffill')

        return df

class HSCodesPipeline(EasyPipeline):
    @staticmethod
    def steps(params):
        
        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))

        dtype = {
            'chapter':          'UInt8',
            'chapter_es':       'String',
            'chapter_en':       'String',
            'chapter_es_short': 'String',
            'chapter_en_short': 'String',
            'hs2_id':           'UInt16',
            'hs2_es':           'String',
            'hs2_en':           'String',
            'hs2_es_short':     'String',
            'hs2_en_short':     'String'
        }
        download_step = DownloadStep(
            connector='hs6-2012',
            connector_path='conns.yaml'
        )
        transform_step = TransformStep()
        load_step = LoadStep('dim_shared_hs12_2digit', db_connector, if_exists='drop', pk=['hs2_id', 'chapter'], dtype=dtype)

        return [download_step, transform_step, load_step]