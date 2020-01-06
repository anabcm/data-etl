
import nltk
import pandas as pd
from bamboo_lib.models import PipelineStep
from bamboo_lib.models import EasyPipeline
from bamboo_lib.connectors.models import Connector
from bamboo_lib.steps import LoadStep

from sklearn.feature_extraction import stop_words

class ReadStep(PipelineStep):
    def run_step(self, prev, params):
        url = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vRf6ecVlEDTaBNfp2VSd7Ti-AnAQDyQlMjF7uek-cQHQ49ihWv4zeSXgN8z0gJV72ogir3hYvYTu8iX/pub?output=xlsx'
        df = pd.read_excel(url, dtype='str')
        return df

class CleanStep(PipelineStep):
    def run_step(self, prev, params):
        df = prev
        cols_es = ['sector_es', 'subsector_es', 'industry_group_es', 'naics_industry_es', 'national_industry_es']
        cols_en = ['sector_en', 'subsector_en', 'industry_group_en', 'naics_industry_en', 'national_industry_en']
        
        #spanish
        nltk.download('stopwords')
        stopwords_es = nltk.corpus.stopwords.words('spanish')
        for ele in cols_es:
            df[ele] = df[ele].str.title()
            for ene in stopwords_es:
                df[ele] = df[ele].str.replace(' ' + ene.title() + ' ', ' ' + ene + ' ')

        #english
        for ele in cols_en:
            df[ele] = df[ele].str.title()
            for ene in list(stop_words.ENGLISH_STOP_WORDS):
                df[ele] = df[ele].str.replace(' ' + ene.title() + ' ', ' ' + ene + ' ')

        return df

class IndustryPipeline(EasyPipeline):
    @staticmethod
    def steps(params, **kwargs):
        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))
        dtype = {
            'sector_id':            'String',
            'sector_es':            'String',
            'sector_en':            'String',
            'subsector_id':         'String',
            'subsector_es':         'String',
            'subsector_en':         'String',
            'industry_group_id':    'String',
            'industry_group_es':    'String',
            'industry_group_en':    'String',
            'naics_industry_id':    'String',
            'naics_industry_es':    'String',
            'naics_industry_en':    'String',
            'national_industry_id': 'String',
            'national_industry_es': 'String',
            'national_industry_en': 'String'
        }

        read_step = ReadStep()
        clean_step = CleanStep()
        load_step = LoadStep('dim_shared_industry', db_connector, if_exists='drop', pk=['industry_group_id', 'subsector_id', 'sector_id'], dtype=dtype)
        
        return [read_step, clean_step, load_step]