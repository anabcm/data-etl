columns = {'sexo': 'sex',
           'edad': 'age',
           'cve_ent': 'ent_id',
           'cve_mun': 'mun_id',
           'ap4_3_3': 'security_perception_in_their_state',
           'ap4_12': 'expenses_in_protection_against_crime',
           'ap5_2_1': 'neighbors_trust',
           'ap5_2_2': 'coworkers_trust',
           'ap5_2_3': 'family_trust',
           'ap5_2_4': 'friends_trust',
           'ap5_4_01': 'traffic_police_trust',
           'ap5_4_02': 'municipal_preventive_police_trust',
           'ap5_4_03': 'state_police_trust',
           'ap5_4_04': 'federal_police_trust',
           'ap5_4_05': 'ministerial_or_judicial_police_trust',
           'ap5_4_06': 'public_ministry_and_state_prosecutors_trust',
           'ap5_4_07': 'state_prosecutor_of_the_republic_trust',
           'ap5_4_08': 'army_trust',
           'ap5_4_09': 'navy_trust',
           'ap5_4_10': 'judges_trust',
           'fac_hog': 'homes_factor',
           'fac_ele': 'people_factor',
           'estrato': 'sociodemographic_stratum'
          }

import pandas as pd
from simpledbf import Dbf5
from bamboo_lib.models import PipelineStep
from bamboo_lib.models import Parameter, EasyPipeline
from bamboo_lib.connectors.models import Connector
from bamboo_lib.steps import LoadStep, DownloadStep

class ReadStep(PipelineStep):
    def run_step(self, prev, params):
        # foreign trade data
        dbf = Dbf5(prev, codec='latin-1')
        df = dbf.to_dataframe()
        df.columns = df.columns.str.lower()
        return df

class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        df = prev
        
        df = df[list(columns.keys())].copy()
        
        #codes
        neighborhood = {'0': '2'}
        for col in df.columns[df.columns.str.contains('ap4_5')]:
            df[col].replace(neighborhood, inplace=True)
            
        df.rename(columns=columns, inplace=True)
        
        for col in ['homes_factor', 'people_factor', 'expenses_in_protection_against_crime']:
            df[col] = df[col].astype('float')

        # income interval replace
        url = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vR08Js9Sh4nNTMe5uBcsDUFedG5MOjIf90p6EHAr1_sWY5kpnI3xUvyPHzQpTEUrXz1pskaoc0uyea6/pub?output=xlsx'
        income = pd.read_excel(url, sheet_name='income', encoding='latin-1')
        col = 'expenses_in_protection_against_crime'
        for ing in df[col].unique():
            for level in range(income.shape[0]):
                if (ing >= income.interval_lower[level]) & (ing < income.interval_upper[level]):
                    df[col] = df[col].replace(ing, str(income.id[level]))
                    break
                if ing >= income.interval_upper[income.shape[0]-1]:
                    df[col] = df[col].replace(ing, str(income.id[income.shape[0]-1]))
                    break
        
        df.fillna('temp', inplace=True)
        
        # municipality id
        df['mun_id'] = df['ent_id'] + df['mun_id']
        
        df.drop(columns=['ent_id'], inplace=True)

        # subset dataset
        df = df[['security_perception_in_their_state', 'sociodemographic_stratum', 'expenses_in_protection_against_crime', 'neighbors_trust', 'coworkers_trust',
            'family_trust', 'friends_trust', 'traffic_police_trust', 'municipal_preventive_police_trust', 'state_police_trust', 'federal_police_trust',
            'ministerial_or_judicial_police_trust', 'public_ministry_and_state_prosecutors_trust', 'state_prosecutor_of_the_republic_trust', 'army_trust',
            'navy_trust', 'judges_trust', 'mun_id', 'sex', 'age', 'homes_factor', 'people_factor']].copy()
        
        df = df.groupby(list(df.columns[~df.columns.str.contains('factor')])).sum().reset_index(col_fill='ffill')
        
        df.replace('temp', pd.np.nan, inplace=True)
        
        # data types
        for col in df.columns:
            try:
                df[col] = df[col].astype('float')
            except:
                continue
        
        return df

class ENVIPEPipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return [
            Parameter(label="Year", name="year", dtype=str)
        ]

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))
        
        dtype = {
            'sex': 'UInt8', 
            'age': 'UInt8', 
            'mun_id': 'UInt16', 
            'security_perception_in_their_state': 'UInt8', 
            'sociodemographic_stratum': 'UInt8',
            'expenses_in_protection_against_crime': 'UInt8',
            'neighbors_trust': 'UInt8',
            'coworkers_trust': 'UInt8',
            'family_trust': 'UInt8',
            'friends_trust': 'UInt8',
            'traffic_police_trust': 'UInt8',
            'municipal_preventive_police_trust': 'UInt8',
            'state_police_trust': 'UInt8',
            'federal_police_trust': 'UInt8',
            'ministerial_or_judicial_police_trust': 'UInt8',
            'public_ministry_and_state_prosecutors_trust': 'UInt8',
            'state_prosecutor_of_the_republic_trust': 'UInt8',
            'army_trust': 'UInt8',
            'navy_trust': 'UInt8',
            'judges_trust': 'UInt8',
            'homes_factor': 'UInt16', 
            'people_factor': 'UInt16',
        }

        download_step = DownloadStep(
            connector='envipe-data',
            connector_path='conns.yaml'
        )
        
        read_step = ReadStep()
        transform_step = TransformStep()
        load_step = LoadStep('inegi_envipe', db_connector, if_exists='append', pk=['mun_id'], 
                             dtype=dtype, nullable_list=['expenses_in_protection_against_crime',
                                                        'traffic_police_trust', 'municipal_preventive_police_trust',
                                                        'state_police_trust', 'federal_police_trust',
                                                        'ministerial_or_judicial_police_trust', 'public_ministry_and_state_prosecutors_trust',
                                                        'state_prosecutor_of_the_republic_trust', 'army_trust', 
                                                        'navy_trust', 'judges_trust'])

        return [download_step, read_step, transform_step, load_step]