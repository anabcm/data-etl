#!/usr/bin/env python
# coding: utf-8

# ### Dim Industry Dimensions Data

import pandas as pd
from bamboo_lib.models import PipelineStep
from bamboo_lib.models import EasyPipeline
from bamboo_lib.connectors.models import Connector
from bamboo_lib.steps import LoadStep

from sklearn.feature_extraction import stop_words

class ReadStep(PipelineStep):
    def run_step(self, prev, params):
        url = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vQT1vtQutSWbdqZKLQokDIHq410zqU3fMTuqdhqc2A84DjI0IlTQb0nqu4h-oIxE7b2oza6S0_Tcpqv/pub?output=xlsx'
        df = pd.read_excel(url, dtype='str')
        df.drop(columns=['naics_industry_id', 'naics_industry_es', 'naics_industry_en', 'national_industry_id', 'national_industry_es', 'national_industry_en'], inplace=True)
        return df

class CleanStep(PipelineStep):
    def run_step(self, prev, params):
        df = prev
        cols_es = ['sector_es', 'subsector_es', 'industry_group_es']
        cols_en = ['sector_en', 'subsector_en', 'industry_group_en']
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
        
        for col in ['sector_id', 'subsector_id', 'industry_group_id']:
            df[col] = df[col].astype('int')

        return df

class CoveragePipeline(EasyPipeline):
    @staticmethod
    def description():
        return 'Processes SCIAN codes from Mexico'

    @staticmethod
    def website():
        return 'http://datawheel.us'

    @staticmethod
    def steps(params, **kwargs):
        # Use of connectors specified in the conns.yaml file
        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))
        dtype = {
            'sector_id':            'UInt8',
            'sector_es':            'String',
            'sector_en':            'String',
            'subsector_id':         'UInt16',
            'subsector_es':         'String',
            'subsector_en':         'String',
            'industry_group_id':    'UInt16',
            'industry_group_es':    'String',
            'industry_group_en':    'String',
        }

        # Definition of each step
        read_step = ReadStep()
        clean_step = CleanStep()
        load_step = LoadStep('dim_shared_industry_enoe', db_connector, if_exists='drop', pk=['industry_group_id', 'subsector_id', 'sector_id'], dtype=dtype)
        
        return [read_step, clean_step, load_step]

