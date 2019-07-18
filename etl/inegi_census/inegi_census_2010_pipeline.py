#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from simpledbf import Dbf5
from bamboo_lib.models import PipelineStep
from bamboo_lib.models import Parameter, EasyPipeline
from bamboo_lib.connectors.models import Connector
from bamboo_lib.steps import LoadStep

class ReadStep(PipelineStep):
    def run_step(self, prev, params):
        # foreign trade data
        url = 'viviendas_15.dbf'
        dbf = Dbf5(url, codec='latin-1')
        df = dbf.to_dataframe()
        df.columns = df.columns.str.lower()
        
        dbf = Dbf5('personas_15.dbf', codec='latin-1')
        dfp = dbf.to_dataframe()
        dfp.columns = dfp.columns.str.lower()
        dfp = dfp[['id_viv', 'ayuprogob', 'ayupeop']].groupby(['id_viv', 'ayuprogob', 'ayupeop']).sum().reset_index(col_fill='ffill')
        df = df.merge(dfp, left_on='id_viv', right_on ='id_viv')
        return df

class CleanStep(PipelineStep):
    def run_step(self, prev, params):
        df = prev
        df = df[['ent', 'mun', 'loc50k', 'clavivp', 'fadqui', 'paredes', 'techos', 'pisos', 'cuadorm', 'totcuart', 'numpers', 'ingtrhog', 'factor', 'ayuprogob', 'ayupeop']]
        # data type conversion
        dtypes = {
            'ent': 'str',
            'mun': 'str',
            'loc50k': 'str',
            'clavivp': 'int',
            'fadqui': 'int',
            'paredes': 'int',
            'techos': 'int',
            'pisos': 'int',
            'cuadorm': 'int',
            'totcuart': 'int',
            'numpers': 'int',
            'ingtrhog': 'float',
            'factor': 'int',
            'ayuprogob': 'int',
            'ayupeop': 'int'
        }
        for key, val in dtypes.items():
            try:
                df.loc[:, key] = df[key].astype(val)
                continue
            except:
                # if contains nan values, values will contain 1.0 format.
                df.loc[:, key] = df[key].astype('float')
        return df

class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        df = prev
        
        # data to replace
        url = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vR08Js9Sh4nNTMe5uBcsDUFedG5MOjIf90p6EHAr1_sWY5kpnI3xUvyPHzQpTEUrXz1pskaoc0uyea6/pub?output=xlsx'
        data = {}
        for col in ['clavivp_2010', 'paredes', 'techos', 'pisos', 'cuadorm', 'totcuart']:
            data[col] = pd.read_excel(url, sheet_name=col, encoding='latin-1', dtype='object')
        data['clavivp'] = data.pop('clavivp_2010')

        # location id
        df['loc_id'] = (df.ent.astype('str') + df.mun.astype('str') + df.loc50k.astype('str')).astype('int')
        df.drop(columns=['ent', 'mun', 'loc50k'], inplace=True)
        df.ingtrhog = df.ingtrhog.fillna(-5).round(0).astype('int64')
        
        # income interval replace
        url = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vR08Js9Sh4nNTMe5uBcsDUFedG5MOjIf90p6EHAr1_sWY5kpnI3xUvyPHzQpTEUrXz1pskaoc0uyea6/pub?output=xlsx'
        income = pd.read_excel(url, sheet_name='income', encoding='latin-1')
        for ing in df.ingtrhog.unique():
            for level in range(income.shape[0]):
                if (ing >= income.interval_lower[level]) & (ing < income.interval_upper[level]):
                    df.ingtrhog = df.ingtrhog.replace(ing, str(income.id[level]))
                    break
                if ing >= income.interval_upper[income.shape[0]-1]:
                    df.ingtrhog = df.ingtrhog.replace(ing, str(income.id[income.shape[0]-1]))
                    break
        df.ingtrhog = df.ingtrhog.astype('int')
        df.ingtrhog.replace(-5, pd.np.nan, inplace=True)
        
        # ids replace
        for col in data.keys():
            df[col] = df[col].replace(dict(zip(data[col]['prev_id'], data[col]['id'].astype('int'))))
        
        # groupby data
        df.fillna('temp', inplace=True)
        df = df.groupby(['loc_id', 'factor', 'clavivp', 'fadqui', 'paredes', 'techos', 'pisos', 'cuadorm', 'totcuart', 'numpers', 'ingtrhog', 'ayuprogob', 'ayupeop']).sum().reset_index(col_fill='ffill')
        df = df.rename(columns={'factor': 'inhabitants', 
                                'clavivp': 'home_type',
                                'fadqui': 'acquisition',
                                'paredes': 'wall', 
                                'techos': 'roof',
                                'pisos': 'floor',
                                'cuadorm': 'bedrooms',
                                'totcuart': 'total_rooms',
                                'numpers': 'n_inhabitants',
                                'ingtrhog': 'income', 
                                'ayuprogob': 'government_financial_aid', 
                                'ayupeop': 'foreign_financial_aid'})

        df.replace('temp', pd.np.nan, inplace=True)
        
        # data type conversion
        for col in df.columns:
            df[col] = df[col].astype('object')
        
        df['year'] = 2010
        df['debt'] = pd.np.nan
        df['coverage'] = pd.np.nan
        df['funding'] = pd.np.nan
        return df

class CoveragePipeline(EasyPipeline):
    @staticmethod
    def pipeline_id():
        return 'program-coverage-pipeline'

    @staticmethod
    def name():
        return 'Program Coverage Pipeline'

    @staticmethod
    def description():
        return 'Processes information from 2010 Mexico Census'

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
            'loc_id':                   'UInt32',
            'inhabitants':              'UInt16', 
            'home_type':                'UInt8',
            'acquisition':              'UInt8',
            'wall':                     'UInt8', 
            'roof':                     'UInt8',
            'floor':                    'UInt8',
            'bedrooms':                 'UInt8',
            'total_rooms':              'UInt8',
            'n_inhabitants':            'UInt8',
            'income':                   'UInt8', 
            'government_financial_aid': 'UInt8', 
            'foreign_financial_aid':    'UInt8',
            'debt':                     'UInt8',
            'coverage':                 'UInt8',
            'funding':                  'UInt8'
        }

        # Definition of each step
        read_step = ReadStep()
        clean_step = CleanStep()
        transform_step = TransformStep()
        load_step = LoadStep('inegi_housing', db_connector, if_exists='append', pk=['loc_id'], nullable_list=['inhabitants', 'home_type', 'acquisition', 'wall', 'roof', 'floor', 'funding',
                                                                                                              'bedrooms', 'total_rooms', 'n_inhabitants', 'income', 'coverage',
                                                                                                              'government_financial_aid', 'foreign_financial_aid', 'debt'], dtype=dtype)
        
        return [read_step, clean_step, transform_step, load_step]