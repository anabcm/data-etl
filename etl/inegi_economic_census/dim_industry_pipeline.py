#!/usr/bin/env python
# coding: utf-8

# ### Dim Industry Dimensions Data

# In[ ]:


import pandas as pd
from bamboo_lib.models import PipelineStep, AdvancedPipelineExecutor
from bamboo_lib.models import Parameter, BasePipeline
from bamboo_lib.connectors.models import Connector
from bamboo_lib.steps import LoadStep
from bamboo_lib.helpers import grab_connector

from sklearn.feature_extraction import stop_words

class ReadStep(PipelineStep):
    def run_step(self, prev, params):
        url = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vQT1vtQutSWbdqZKLQokDIHq410zqU3fMTuqdhqc2A84DjI0IlTQb0nqu4h-oIxE7b2oza6S0_Tcpqv/pub?output=xlsx'
        df = pd.read_excel(url, dtype='str')
        return df

class CleanStep(PipelineStep):
    def run_step(self, prev, params):
        df = prev
        cols_es = ['sector_es', 'subsector_es', 'industry_group_es', 'naics_industry_es', 'national_industry_es']
        cols_en = ['sector_en', 'subsector_en', 'industry_group_en', 'naics_industry_en', 'national_industry_en']
        stopwords_es = ['a', 'ante', 'con', 'contra', 'de', 'desde', 'la', 'lo', 'las', 'los', 'y']
        
        #spanish
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

class CoveragePipeline(BasePipeline):
    @staticmethod
    def pipeline_id():
        return 'program-coverage-pipeline-temp'

    @staticmethod
    def name():
        return 'Program Coverage Pipeline temp'

    @staticmethod
    def description():
        return 'Processes information from Mexico'

    @staticmethod
    def website():
        return 'http://datawheel.us'

    @staticmethod
    def parameter_list():
        return [
            Parameter(label='Source connector', name='source-connector', dtype=str, source=Connector)
        ]

    @staticmethod
    def run(params, **kwargs):
        # Use of connectors specified in the conns.yaml file
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

        # Definition of each step
        step0 = ReadStep()
        step1 = CleanStep()
        step2 = LoadStep('dim_shared_industry', db_connector, if_exists='drop', pk=['national_industry_id'], dtype=dtype)

        # Definition of the pipeline and its steps
        pipeline = AdvancedPipelineExecutor(params)
        pipeline = pipeline.next(step0).next(step1).next(step2)
        
        return pipeline.run_pipeline()


def run_coverage(params, **kwargs):
    pipeline = CoveragePipeline()
    pipeline.run(params)


if __name__ == '__main__':
    run_coverage({
        'database-connector': 'clickhouse'
    })

