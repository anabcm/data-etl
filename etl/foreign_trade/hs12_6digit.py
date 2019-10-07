def format_text(df, cols_names=None, stopwords=None):

    # format
    for ele in cols_names:
        df[ele] = df[ele].str.title().str.strip()
        for ene in stopwords:
            df[ele] = df[ele].str.replace(' ' + ene.title() + ' ', ' ' + ene + ' ')

    return df

import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, PipelineStep, Parameter
from bamboo_lib.steps import LoadStep
from sklearn.feature_extraction import stop_words

class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        # read data
        df = pd.read_csv('https://docs.google.com/spreadsheets/d/e/2PACX-1vT0959aScOQnJcoxJTgvPqwma0jxsdyGZGswl4z8yl9KqiPeZleckFHoFyA2KHCMP3HrE8n7EwLyQAR/pub?output=csv')
        url = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vTeFs5_Cv49nNo4leJAMwoUZU_smOPwjCnoRprDuqkOVWB7UZSqm3j16mXv6N0aRRek-rtlikP5ZJ46/pub?output=xlsx'
        exports = pd.read_excel(url, sheet_name='HS6 Exports')
        imports = pd.read_excel(url, sheet_name='HS6 Imports')
        exports.columns = exports.columns.str.lower()
        imports.columns = imports.columns.str.lower()
        top_hs = exports.append(imports)

        cols_es = ['chapter_es', 'hs2_es', 'hs4_es', 'hs6_es']
        cols_en = ['chapter_en', 'hs2_en', 'hs4_en', 'hs6_en']

        # codes ids
        stopwords_es = ['a', 'e', 'en', 'ante', 'con', 'contra', 'de', 'del', 'desde', 'la', 'lo', 'las', 'los', 'y']
        df = format_text(df, cols_es, stopwords=stopwords_es)
        df = format_text(df, cols_en, stopwords=stop_words.ENGLISH_STOP_WORDS)

        for col in ['hs6_id', 'hs4_id', 'hs2_id', 'chapter']:
            df[col] = df[col].astype('int')

        df['hs6_es_short'] = df['hs6_es']
        df['hs6_en_short'] = df['hs6_en']

        for ele in top_hs['hs6 id'].unique():
            df.loc[df.hs6_id == ele, 'hs6_es_short'] = top_hs.loc[top_hs['hs6 id'] == ele, 'name_es'].values[0]
            df.loc[df.hs6_id == ele, 'hs6_en_short'] = top_hs.loc[top_hs['hs6 id'] == ele, 'name_en'].values[0]

        return df

class HSCodesPipeline(EasyPipeline):
    @staticmethod
    def steps(params):
        
        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))

        dtype = {
            'chapter':      'UInt8',
            'chapter_es':   'String',
            'chapter_en':   'String',
            'hs2_id':       'UInt16',
            'hs2_es':       'String',
            'hs2_en':       'String',
            'hs4_id':       'UInt32',
            'hs4_es':       'String',
            'hs4_en':       'String',
            'hs6_id':       'UInt32',
            'hs6_es':       'String',
            'hs6_en':       'String',
            'hs6_es_short': 'String',
            'hs6_en_short': 'String'
        }
        
        transform_step = TransformStep()
        load_step = LoadStep('dim_shared_hs12_6digit', db_connector, if_exists='drop', pk=['hs6_id', 'hs4_id', 'hs2_id', 'chapter'], dtype=dtype)

        return [transform_step, load_step]