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
        df.columns = df.columns.str.lower().str.replace('suma de ', '')
        # external ids
        url = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vTzv8dN6-Cn7vR_v9UO5aPOBqumAy_dXlcnVOFBzxCm0C3EOO4ahT5FdIOyrtcC7p-akGWC_MELKTcM/pub?output=xlsx'
        ent = pd.read_excel(url, sheet_name='origin', dtypes='str')
        return df, ent

class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        df, ent = prev[0], prev[1]
        # type format
        for col in ['ent_id', 'mun_id', 'career', 'type', 'period', 'institution']:
            df[col] = df[col].ffill()
        df.ent_id = df.ent_id.str.title()
          
        # totals clean
        df = df.loc[~df.program.isna()]
        
        # ids replace from external table
        df.ent_id.replace(dict(zip(ent.origin, ent.id)), inplace=True)
        
        # municipality level id
        df.loc[:, 'mun_id'] = df.loc[:, 'ent_id'].astype('str') + df.loc[:, 'mun_id'].astype('str')
        df.drop(columns=['ent_id'], inplace=True)
        
        # column names format
        df.columns = df.columns.str.replace('suma de ', '').str.replace('pni-', '')

        # melt step
        df = df[['mun_id', 'career', 'type', 'period', 'institution', 'program',
               'mat-h-22', 'mat-h-23', 'mat-h-24', 'mat-h-25', 'mat-h-26', 'mat-h-27',
               'mat-h-28', 'mat-h-29', 'mat-h-30', 'mat-h-31', 'mat-h-32', 'mat-m-22',
               'mat-m-23', 'mat-m-24', 'mat-m-25', 'mat-m-26', 'mat-m-27', 'mat-m-28',
               'mat-m-29', 'mat-m-30', 'mat-m-31', 'mat-m-32']].copy()
        
        df.columns = df.columns.str.replace('mat-', '')
        
        df = df.melt(id_vars=['mun_id', 'career', 'type', 'period', 'institution', 'program'], var_name='sex', value_name='value')
        df = df.loc[df.value != 0]
        
        split = df['sex'].str.split('-', n=1, expand=True) 
        df['sex'] = split[0]
        df['age'] = split[1]
        
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
        
        for col in ['mun_id', 'career', 'program', 'type', 'sex', 'value', 'age']:
            df[col] = df[col].astype('float')

        df.drop(columns=['career'], inplace=True)
        
        return df

class EnrollmentPipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return [
            Parameter(name='url', dtype=str)
        ]

    @staticmethod
    def steps(params):
        
        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))
        
        dtype = {
            'mun_id':      'UInt16',
            'type':        'UInt8',
            'period':      'String',
            'institution': 'String',
            'program':     'UInt64',
            'sex':         'UInt8',
            'value':       'UInt16',
            'age':         'UInt8'
        }
        
        read_step = ReadStep()
        transform_step = TransformStep()
        load_step = LoadStep('anuies_postgrade_enrollment', db_connector, if_exists='append', pk=['mun_id', 'institution', 'program'], dtype=dtype)

        return [read_step, transform_step, load_step]