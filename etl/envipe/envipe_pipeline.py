columns = {'sexo': 'sex',
           'edad': 'age',
           'cve_ent': 'ent_id',
           'cve_mun': 'mun_id',
           'aream': 'metropolitan_area',
           'ap4_3_3': 'security_perception_in_their_state',
           'ap4_4_01': 'security_perception_in_house',
           'ap4_4_02': 'security_perception_in_work',
           'ap4_4_03': 'security_perception_in_street',
           'ap4_4_04': 'security_perception_in_school',
           'ap4_4_05': 'security_perception_in_market',
           'ap4_4_06': 'security_perception_in_mall',
           'ap4_4_07': 'security_perception_in_bank',
           'ap4_4_08': 'security_perception_in_ATM',
           'ap4_4_09': 'security_perception_in_public_transport',
           'ap4_4_10': 'security_perception_in_the_car',
           'ap4_4_11': 'security_perception_in_the_highway',
           'ap4_4_12': 'security_perception_in_park_recreation_center',
           'ap4_5_01': 'alcohol_consumption_in_street_near_neighborhood',
           'ap4_5_02': 'gangs_near_neighborhood',
           'ap4_5_03': 'fights_between_neighbors_near_neighborhood',
           'ap4_5_04': 'illegal_alcohol_consumption_near_neighborhood',
           'ap4_5_05': 'counterfeit_product_marketing_near_neighborhood',
           'ap4_5_06': 'police_violence_against_citizens_near_neighborhood',
           'ap4_5_07': 'invasion_of_properties_and_real_estate_near_neighborhood',
           'ap4_5_08': 'drog_use_near_neighborhood',
           'ap4_5_09': 'frequent_robberies_near_neighborhood',
           'ap4_5_10': 'consumption_of_drugs_near_neighborhood',
           'ap4_5_11': 'frecuent_shots_near_neighborhood',
           'ap4_5_12': 'prostitution_near_neighborhood',
           'ap4_5_13': 'kidnappings_near_neighborhood',
           'ap4_5_14': 'homicides_near_neighborhood',
           'ap4_5_15': 'extortion_near_neighborhood',
           'ap4_5_16': 'none',
           'ap4_5_99': 'not_specified',
           'ap4_11_01': 'changing_doors_or_windows_protection_measures',
           'ap4_11_02': 'change_or_place_locks_protection_measures',
           'ap4_11_03': 'place_or_reinforce_bars_or_fences_protection_measures',
           'ap4_11_04': 'install_alarms_or_surveillance_cameras_protection_measures',
           'ap4_11_05': 'private_surveillance_protection_measures',
           'ap4_11_06': 'actions_with_neighbors_protection_measures',
           'ap4_11_07': 'insurance_contracting_protection_measures',
           'ap4_11_08': 'buy_guard_dog_protection_measures',
           'ap4_11_09': 'acquire_guns_protection_measures',
           'ap4_11_10': 'move_out_protection_measures',
           'ap4_11_11': 'other_measure',
           'ap4_12': 'expenses_in_protection_against_crime',
           'ap5_2_1': 'neighbors_trust',
           'ap5_2_2': 'coworkers_trust',
           'ap5_2_3': 'family_trust',
           'ap5_2_4': 'friends_trust',
           'ap5_3_01': 'traffic_police_identification',
           'ap5_3_02': 'municipal_preventive_police_identification',
           'ap5_3_03': 'state_police_identification',
           'ap5_3_04': 'federal_police_identification',
           'ap5_3_05': 'ministerial_or_judicial_police_identification',
           'ap5_3_06': 'public_ministry_and_state_prosecutors_identification',
           'ap5_3_07': 'state_prosecutor_of_the_republic_identification',
           'ap5_3_08': 'army_identification',
           'ap5_3_09': 'navy_identification',
           'ap5_3_10': 'judges_identification',
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
           'ap5_5_01': 'traffic_police_corruption_perception',
           'ap5_5_02': 'municipal_preventive_police_corruption_perception',
           'ap5_5_03': 'state_police_corruption_perception',
           'ap5_5_04': 'federal_police_corruption_perception',
           'ap5_5_05': 'ministerial_or_judicial_police_corruption_perception',
           'ap5_5_06': 'public_ministry_and_state_prosecutors_corruption_perception',
           'ap5_5_07': 'state_prosecutor_of_the_republic_corruption_perception',
           'ap5_5_08': 'army_corruption_perception',
           'ap5_5_09': 'navy_corruption_perception',
           'ap5_5_10': 'judges_corruption_perception',
           'ap5_6_01': 'traffic_police_trust_performance_perception',
           'ap5_6_02': 'municipal_preventive_police_performance_perception',
           'ap5_6_03': 'state_police_performance_perception',
           'ap5_6_04': 'federal_police_performance_perception',
           'ap5_6_05': 'ministerial_or_judicial_police_performance_perception',
           'ap5_6_06': 'public_ministry_and_state_prosecutors_performance_perception',
           'ap5_6_07': 'state_prosecutor_of_the_republicperformance_perception',
           'ap5_6_08': 'army_performance_perception',
           'ap5_6_09': 'navy_performance_perception',
           'ap5_6_10': 'judges_performance_perception',
           'ap5_7_1': 'traffic_police_trust_willingness_to_help',
           'ap5_7_2': 'municipal_preventive_police_willingness_to_help',
           'ap5_7_3': 'state_police_willingness_to_help',
           'ap5_7_4': 'federal_police_willingness_to_help',
           'ap5_8': 'trust_in_prisons',
           'fac_hog': 'homes_factor',
           'fac_ele': 'people_factor',
           'fac_hog_am': 'homes_factor_ma',
           'fac_ele_am': 'people_factor_ma', 
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
        
        for col in ['homes_factor', 'people_factor', 'homes_factor_ma', 'people_factor_ma', 'expenses_in_protection_against_crime']:
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
        
        df.drop(columns=['none', 'not_specified', 'homes_factor_ma', 'people_factor_ma', 'ent_id'], inplace=True)
        
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
            'metropolitan_area': 'UInt8',
            'security_perception_in_their_state': 'UInt8', 
            'security_perception_in_house': 'UInt8',
            'security_perception_in_work': 'UInt8', 
            'security_perception_in_street': 'UInt8',
            'security_perception_in_school': 'UInt8', 
            'security_perception_in_market': 'UInt8',
            'security_perception_in_mall': 'UInt8', 
            'security_perception_in_bank': 'UInt8',
            'security_perception_in_ATM': 'UInt8', 
            'security_perception_in_public_transport': 'UInt8',
            'security_perception_in_the_car': 'UInt8', 
            'security_perception_in_the_highway': 'UInt8',
            'security_perception_in_park_recreation_center': 'UInt8',
            'alcohol_consumption_in_street_near_neighborhood': 'UInt8',
            'gangs_near_neighborhood': 'UInt8',
            'fights_between_neighbors_near_neighborhood': 'UInt8',
            'illegal_alcohol_consumption_near_neighborhood': 'UInt8',
            'counterfeit_product_marketing_near_neighborhood': 'UInt8',
            'police_violence_against_citizens_near_neighborhood': 'UInt8',
            'invasion_of_properties_and_real_estate_near_neighborhood': 'UInt8',
            'drog_use_near_neighborhood': 'UInt8', 
            'frequent_robberies_near_neighborhood': 'UInt8',
            'consumption_of_drugs_near_neighborhood': 'UInt8',
            'frecuent_shots_near_neighborhood': 'UInt8', 
            'prostitution_near_neighborhood': 'UInt8',
            'kidnappings_near_neighborhood': 'UInt8', 
            'homicides_near_neighborhood': 'UInt8',
            'extortion_near_neighborhood': 'UInt8',
            'changing_doors_or_windows_protection_measures': 'UInt8',
            'change_or_place_locks_protection_measures': 'UInt8',
            'place_or_reinforce_bars_or_fences_protection_measures': 'UInt8',
            'install_alarms_or_surveillance_cameras_protection_measures': 'UInt8',
            'private_surveillance_protection_measures': 'UInt8',
            'actions_with_neighbors_protection_measures': 'UInt8',
            'insurance_contracting_protection_measures': 'UInt8',
            'buy_guard_dog_protection_measures': 'UInt8', 
            'acquire_guns_protection_measures': 'UInt8',
            'move_out_protection_measures': 'UInt8', 
            'other_measure': 'UInt8',
            'expenses_in_protection_against_crime': 'UInt8', 
            'neighbors_trust': 'UInt8',
            'coworkers_trust': 'UInt8', 
            'family_trust': 'UInt8', 
            'friends_trust': 'UInt8',
            'traffic_police_identification': 'UInt8',
            'municipal_preventive_police_identification': 'UInt8',
            'state_police_identification': 'UInt8', 
            'federal_police_identification': 'UInt8',
            'ministerial_or_judicial_police_identification': 'UInt8',
            'public_ministry_and_state_prosecutors_identification': 'UInt8',
            'state_prosecutor_of_the_republic_identification': 'UInt8',
            'army_identification': 'UInt8', 
            'navy_identification': 'UInt8', 
            'judges_identification': 'UInt8',
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
            'traffic_police_corruption_perception': 'UInt8',
            'municipal_preventive_police_corruption_perception': 'UInt8',
            'state_police_corruption_perception': 'UInt8',
            'federal_police_corruption_perception': 'UInt8',
            'ministerial_or_judicial_police_corruption_perception': 'UInt8',
            'public_ministry_and_state_prosecutors_corruption_perception': 'UInt8',
            'state_prosecutor_of_the_republic_corruption_perception': 'UInt8',
            'army_corruption_perception': 'UInt8', 
            'navy_corruption_perception': 'UInt8',
            'judges_corruption_perception': 'UInt8',
            'traffic_police_trust_performance_perception': 'UInt8',
            'municipal_preventive_police_performance_perception': 'UInt8',
            'state_police_performance_perception': 'UInt8',
            'federal_police_performance_perception': 'UInt8',
            'ministerial_or_judicial_police_performance_perception': 'UInt8',
            'public_ministry_and_state_prosecutors_performance_perception': 'UInt8',
            'state_prosecutor_of_the_republicperformance_perception': 'UInt8',
            'army_performance_perception': 'UInt8', 
            'navy_performance_perception': 'UInt8',
            'judges_performance_perception': 'UInt8',
            'traffic_police_trust_willingness_to_help': 'UInt8',
            'municipal_preventive_police_willingness_to_help': 'UInt8',
            'state_police_willingness_to_help': 'UInt8',
            'federal_police_willingness_to_help': 'UInt8', 
            'trust_in_prisons': 'UInt8',
            'sociodemographic_stratum': 'UInt8', 
            'homes_factor': 'UInt16', 
            'people_factor': 'UInt16',
        }

        download_step = DownloadStep(
            connector='envipe-data',
            connector_path='conns.yaml'
        )
        
        read_step = ReadStep()
        transform_step = TransformStep()
        load_step = LoadStep('envipe', db_connector, if_exists='append', pk=['mun_id'], 
                             dtype=dtype, nullable_list=['metropolitan_area',
                                                        'expenses_in_protection_against_crime',
                                                        'traffic_police_trust', 
                                                        'municipal_preventive_police_trust',
                                                        'state_police_trust', 'federal_police_trust',
                                                        'ministerial_or_judicial_police_trust',
                                                        'public_ministry_and_state_prosecutors_trust',
                                                        'state_prosecutor_of_the_republic_trust', 'army_trust', 'navy_trust',
                                                        'judges_trust', 'traffic_police_corruption_perception',
                                                        'municipal_preventive_police_corruption_perception',
                                                        'state_police_corruption_perception',
                                                        'federal_police_corruption_perception',
                                                        'ministerial_or_judicial_police_corruption_perception',
                                                        'public_ministry_and_state_prosecutors_corruption_perception',
                                                        'state_prosecutor_of_the_republic_corruption_perception',
                                                        'army_corruption_perception', 'navy_corruption_perception',
                                                        'judges_corruption_perception',
                                                        'traffic_police_trust_performance_perception',
                                                        'municipal_preventive_police_performance_perception',
                                                        'state_police_performance_perception',
                                                        'federal_police_performance_perception',
                                                        'ministerial_or_judicial_police_performance_perception',
                                                        'public_ministry_and_state_prosecutors_performance_perception',
                                                        'state_prosecutor_of_the_republicperformance_perception',
                                                        'army_performance_perception', 'navy_performance_perception',
                                                        'judges_performance_perception',
                                                        'traffic_police_trust_willingness_to_help',
                                                        'municipal_preventive_police_willingness_to_help',
                                                        'state_police_willingness_to_help',
                                                        'federal_police_willingness_to_help'])

        return [download_step, read_step, transform_step, load_step]