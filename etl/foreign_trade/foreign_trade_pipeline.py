
import pandas as pd
from bamboo_lib.models import PipelineStep, AdvancedPipelineExecutor
from bamboo_lib.models import Parameter, EasyPipeline
from bamboo_lib.connectors.models import Connector
from bamboo_lib.steps import LoadStep

class ReadStep(PipelineStep):
    def run_step(self, prev, params):
        # foreign trade data
        url = 'https://storage.googleapis.com/datamexico-data/foreign_trade/Commercial_data_mx.csv'
        df = pd.read_csv(url, encoding='latin-1', dtype='object', header=0)
        return df

class CleanStep(PipelineStep):
    def run_step(self, prev, params):
        df = prev
        df.drop(0, inplace=True)
        df.drop(columns=['EN'], inplace=True)
        df.columns = df.columns.str.strip().str.lower()
        for ele in [(' ', '_'), ('(', ''), (')', ''), ('__', '_')]:
            df.columns = df.columns.str.replace(ele[0], ele[1])
        return df

class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        df = prev
        for col in ['unitary_price', 'commercial_value', 'code_of_dispatch_customs_section', 'sequence_of_tariff_fractions', 'code_of_entry_customs_section', 'tariff_fraction', 'postal_code_taxpayer_address', 'commercial_measure_code']:
            df[col] = df[col].astype('float')

        # country codes iso 3166-1 alpha-3
        url = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vSDaqIIMI56NCwzU1fJxz6erC474xtqJBytaBaqVJS6b5Op7nr1p_sE1Fq4XKVaNdDjoz-yOzX1rRj6/pub?output=xlsx'
        data = {}
        for col in ['entidad_federativa', 'country_codes', 'iso_3166']:
            data[col] = pd.read_excel(url, sheet_name=col, encoding='latin-1', dtype='object')
        
        df.country_origin_destiny.replace(dict(zip(data['country_codes']['code'], data['country_codes']['country_en'])), inplace=True)
        df.state_code_taxpayer.replace(dict(zip(data['entidad_federativa']['code'], data['entidad_federativa']['name'])), inplace=True)
        df.country_taxpayer.replace(dict(zip(data['iso_3166']['code'], data['iso_3166']['country_en'])), inplace=True)
        
        # clean stopwords
        stopwords_es = ['a', 'e', 'ante', 'con', 'contra', 'de', 'del', 'desde', 'la', 'lo', 'las', 'los', 'y']
        
        #spanish
        for ele in ['country_origin_destiny']:
            df[ele] = df[ele].str.title()
            for ene in stopwords_es:
                df[ele] = df[ele].str.replace(' ' + ene.title() + ' ', ' ' + ene + ' ')

        # date format
        df.date_scale = df.date_scale.str[6:] + df.date_scale.str[3:5] + df.date_scale.str[:2]
        df.date_scale = df.date_scale.astype('int32')
        df.rename(columns={'date_scale': 'date_id'}, inplace=True)

        return df

class CoveragePipeline(EasyPipeline):
    @staticmethod
    def pipeline_id():
        return 'program-coverage-pipeline-temp'

    @staticmethod
    def name():
        return 'Program Coverage Pipeline temp'

    @staticmethod
    def description():
        return 'Processes information from foreign trade data Mexico'

    @staticmethod
    def website():
        return 'http://datawheel.us'

    @staticmethod
    def parameter_list():
        return [
            Parameter(label='Source connector', name='source-connector', dtype=str, source=Connector)
        ]

    @staticmethod
    def steps(params, **kwargs):
        # Use of connectors specified in the conns.yaml file
        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))

        dtype = {

            'document_code':                    'String',
            'operation_code':                   'String',
            'state_code_taxpayer':              'String',
            'country_taxpayer':                 'String',
            'country_origin_destiny':           'String',
            'customs_patent':                   'String',
            'number_of_petition':               'String',
            
            'commercial_measure_code':          'UInt8',
            'code_of_dispatch_customs_section': 'UInt16',
            'sequence_of_tariff_fractions':     'UInt16',
            'code_of_entry_customs_section':    'UInt16',
            'tariff_fraction':                  'UInt32',
            'postal_code_taxpayer_address':     'UInt32',

            'unitary_price':                    'Float64',
            'commercial_value':                 'Float64',

            'date_id':                          'UInt32'
        }

        # Definition of each step
        read_step = ReadStep()
        clean_step = CleanStep()
        transform_step = TransformStep()
        load_step = LoadStep('foreign_trade', db_connector, if_exists='drop', pk=['date_id'], nullable_list=['state_code_taxpayer', 'postal_code_taxpayer_address'], dtype=dtype)
        
        return [read_step, clean_step, transform_step, load_step]
