
import nltk
import pandas as pd
from bamboo_lib.models import PipelineStep
from bamboo_lib.models import EasyPipeline
from bamboo_lib.connectors.models import Connector
from bamboo_lib.steps import LoadStep, DownloadStep

from sklearn.feature_extraction import stop_words

class ReadStep(PipelineStep):
    def run_step(self, prev, params):
        df = pd.read_csv(prev, dtype='str')
        return df

class CleanStep(PipelineStep):
    def run_step(self, prev, params):
        df = prev
        cols_es = ['occupation_es', 'subgroup_es', 'group_es', 'category_es']
        cols_en = ['occupation_en', 'subgroup_en', 'group_en', 'category_en']
        
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
        
        for col in ['occupation_id', 'subgroup_id', 'group_id', 'category_id']:
            df[col] = df[col].astype('int')

        return df

class CoveragePipeline(EasyPipeline):
    @staticmethod
    def description():
        return 'Processes SINCO codes from Mexico'

    @staticmethod
    def website():
        return 'http://datawheel.us'

    @staticmethod
    def steps(params, **kwargs):
        # Use of connectors specified in the conns.yaml file
        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))
        dtype = {
            'occupation_id': 'UInt16',
            'occupation_es': 'String',
            'occupation_en': 'String',
            'subgroup_id':  'UInt16',
            'subgroup_es':  'String',
            'subgroup_en':  'String',
            'group_id':     'UInt8',
            'group_es':     'String',
            'group_en':     'String',
            'category_id':  'UInt8',
            'category_es':  'String',
            'category_en':  'String'
        }

        # Definition of each step
        download_step = DownloadStep(
            connector="sinco",
            connector_path="conns.yaml"
        )
        read_step = ReadStep()
        clean_step = CleanStep()
        load_step = LoadStep('dim_shared_occupations_enoe', db_connector, if_exists='drop', pk=['occupation_id', 'subgroup_id', 'group_id', 'category_id'], dtype=dtype)
        
        return [download_step, read_step, clean_step, load_step]

