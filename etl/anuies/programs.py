import pandas as pd
from helpers import format_text
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, PipelineStep, Parameter
from bamboo_lib.steps import LoadStep
from sklearn.feature_extraction import stop_words

class ReadStep(PipelineStep):
    def run_step(self, prev, params):
        # careers
        url = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vSry9xO5_KVDA7bWVTKPrFgjnDbU6CP6f9lNrGX5zqfJH3HBc2-3EPeIBfCS92_UyiSnBnt5XeEpb2T/pub?output=csv'
        df = pd.read_csv(url)
        # programs
        url = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vTRqe4aa9Maq0WOZTq6DzpflyyGUhTHMoy5l_nfrrmL0fG0f5ccnRoEDg8klrl1JbynwPuwIuTDhy-z/pub?output=csv'
        df_program = pd.read_csv(url)
        return df, df_program

class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        df, df_program = prev[0], prev[1]

        # stopwords
        stopwords_es = ['a', 'e', 'ante', 'con', 'contra', 'de', 'desde', 'la', 'lo', 'las', 'los', 'y']
        stopwords_en = list(stop_words.ENGLISH_STOP_WORDS)

        for col in ['code', 'area']:
            df[col] = df[col].astype('int')

        # programs
        cols_es = ['area_es', 'field_es', 'subfield_es', 'speciality_es']
        cols_en = ['area_en', 'field_en', 'subfield_en', 'speciality_en']

        df_program = format_text(df_program, cols_names=cols_es, stopwords=stopwords_es)
        df_program = format_text(df_program, cols_names=cols_en, stopwords=stopwords_en)

        for col in ['area_id', 'field_id', 'subfield_id', 'speciality_id']:
            df_program[col] = df_program[col].astype('int')

        # merge
        df = df.merge(df_program, left_on='area', right_on='speciality_id')

        df.drop(columns=['area', 'speciality_id', 'speciality_es', 'speciality_en'], inplace=True)

        return df

class ProgramsCodesPipeline(EasyPipeline):
    @staticmethod
    def description():
        return 'Processes Careers codes from Mexico'

    @staticmethod
    def website():
        return 'http://datawheel.us'

    @staticmethod
    def steps(params, **kwargs):

        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))
        dtype = {
            'area_id':       'UInt8', 
            'area_es':       'String',
            'area_en':       'String',
            'field_id':      'UInt8',
            'field_es':      'String',
            'field_en':      'String',
            'subfield_id':   'UInt16',
            'subfield_es':   'String',
            'subfield_en':   'String',
            'code':          'UInt64',
            'name_es':       'String',
            'name_en':       'String',
        }

        # Definition of each step
        read_step = ReadStep()
        transform_step = TransformStep()
        load_step = LoadStep('dim_shared_careers_anuies', db_connector, if_exists='drop', 
                            pk=['code'], dtype=dtype, engine='ReplacingMergeTree')
        
        return [read_step, transform_step, load_step]