import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, PipelineStep, Parameter
from bamboo_lib.steps import LoadStep

class ReadStep(PipelineStep):
    def run_step(self, prev, params):
        # read data
        df = pd.read_excel(params.get('url'), header=1)
        df.rename(columns={'Unnamed: 0': 'ent_id', 'Unnamed: 1': 'mun_id', 'Unnamed: 2': 'career', 
                           'Unnamed: 3': 'type', 'Unnamed: 4': 'period', 'Unnamed: 5': 'institution', 
                           'Unnamed: 6': 'program'}, inplace=True)
        df.drop(columns=['ent_id', 'mun_id'], inplace=True)
        df.columns = df.columns.str.lower()
        # careers ids
        url = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vTji_9aF8v-wvkRu1G0_1Cgq2NxrEjM0ToMoKWwc2eW_b-aOMXScstb8YDpSt2r6a6iU2AQXpkNlfws/pub?output=csv'
        careers = pd.read_csv(url)
        return df, careers

class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        df, careers = prev[0], prev[1]
        # type format
        for col in ['career', 'type', 'period', 'institution']:
            df[col] = df[col].ffill()

        # totals clean
        df = df.loc[~df.program.isna()]

        # column names format
        df.columns = df.columns.str.replace('suma de ', '')

        # melt step
        df = df[['career', 'type', 'period', 'institution', 'program', 'e-h', 'e-m', 'g-h', 'g-m']].copy()
        df = df.melt(id_vars=['career', 'type', 'period', 'institution', 'program'], var_name='stat', value_name='value')
        df = df.loc[df.value != 0]

        split = df['stat'].str.split('-', n=1, expand=True) 
        df['stat'] = split[0]
        df['sex'] = split[1]

        # encoding
        stat = {
            'e': 1,
            'g': 2
        }
        df.stat.replace(stat, inplace=True)

        sex = {
            'h': 1,
            'm': 2,
        }
        df.sex.replace(sex, inplace=True)

        types = {
            'MAESTRÍA': 1,
            'ESPECIALIDAD': 2,
            'DOCTORADO': 3
        }
        df.type.replace(types, inplace=True)

        # careers ids
        df.program = df.program.str.strip().str.replace('  ', ' ').str.replace(':', '')
        df.program.replace(dict(zip(careers.name_es, careers.code)), inplace=True)

        for col in ['career', 'program', 'type', 'sex', 'value', 'stat']:
            df[col] = df[col].astype('float')

        df.drop(columns=['career'], inplace=True)
        print(df.period.unique())
        return df

class StatusPipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return [
            Parameter(name='url', dtype=str)
        ]

    @staticmethod
    def steps(params):
        
        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))
        
        dtype = {
            'type':        'UInt8',
            'period':      'String',
            'institution': 'String',
            'program':     'UInt64',
            'stat':        'UInt8',
            'value':       'UInt32',
            'sex':         'UInt8'
        }
        
        read_step = ReadStep()
        transform_step = TransformStep()
        load_step = LoadStep('anuies_postgraduate_status', db_connector, if_exists='append', pk=['institution', 'program'], dtype=dtype)

        return [read_step, transform_step, load_step]