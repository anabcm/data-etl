def format_text(df, cols_names=None, stopwords=None):

    # format
    for ele in cols_names:
        df[ele] = df[ele].str.title()
        for ene in stopwords:
            df[ele] = df[ele].str.replace(' ' + ene.title() + ' ', ' ' + ene + ' ')

    return df

import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, PipelineStep, Parameter
from bamboo_lib.steps import LoadStep

class ReadStep(PipelineStep):
    def run_step(self, prev, params):
        # read data
        url = 'https://storage.googleapis.com/datamexico-data/anuies/work_centers/' + params.get('index').zfill(2) + '.xlsx'
        df = pd.read_excel(url, usecols=[0,2,3,4,5,6,7,8,9,11,13,38,39], dtypes='str')
        df.columns = ['code', 'modalidad', 'name', 'educational_type', 'educational_level', 
                      'educational_service', 'name_control', 'type_support', 'ent_id', 'mun_id', 'loc_id', 'longitude', 'latitude']
        return df

class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        df = prev
        # type format
        df = df.loc[df.educational_type == 'SUPERIOR'].copy()
        df.drop(columns=['educational_type', 'educational_level', 'educational_service', 
                         'modalidad', 'ent_id', 'mun_id', 'longitude', 'latitude', 'loc_id'], inplace=True)
        # encoding
        name_control = {
            'PRIVADO': 1,
            'PÚBLICO': 2
        }
        df.name_control.replace(name_control, inplace=True)

        type_support =  {
            'ESTATAL': 1, 
            'SUBSIDIO': 2,
            'AUTÓNOMO': 3, 
            'PRIVADO': 4,
            'FEDERAL': 5,
            'FEDERAL TRANSFERIDO': 6, 
            'FEDERAL TRANSFERIDO-ESTATAL': 7
        }
        df.type_support.replace(type_support, inplace=True)

        df.name = df.name.str.replace('"', '')

        # stopwords
        stopwords_es = ['a', 'e', 'ante', 'con', 'contra', 'de', 'desde', 'la', 'lo', 'las', 'los', 'y']

        cols_es = ['name']

        #format
        df = format_text(df, cols_names=cols_es, stopwords=stopwords_es)

        return df

class WorkCentersPipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return [
            Parameter(name='index', dtype=str)
        ]
    @staticmethod
    def steps(params):
        
        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))
        
        dtype = {
            'code':         'String',
            'name':         'String',
            'name_control': 'UInt8',
            'type_support': 'UInt8',
        }
        
        read_step = ReadStep()
        transform_step = TransformStep()
        load_step = LoadStep('dim_work_centers', db_connector, if_exists='drop', pk=['code'], dtype=dtype, engine='ReplacingMergeTree')

        return [read_step, transform_step, load_step]