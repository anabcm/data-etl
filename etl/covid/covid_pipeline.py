
import os
import time
import glob
import zipfile
import numpy as np
import pandas as pd
from datetime import datetime
from bamboo_lib.helpers import grab_parent_dir
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, PipelineStep, Parameter
from bamboo_lib.steps import DownloadStep, LoadStep, UnzipToFolderStep
from shared import rename_columns, RENAME_COUNTRIES, clean_tables
from bamboo_lib.helpers import query_to_df
from dim_time_date import DimTimeDatePipeline
from dim_time_covid_stats import DimTimeDateCovidStatsPipeline
from etl.helpers import norm

class TransformStep(PipelineStep):
    def run_step(self, prev, params):

        COLS_SUBSET = list(rename_columns.keys()) + ['municipio_res', 'entidad_res', 'id_registro', 'clasificacion_final']

        df = params.get('chunk')

        df.columns = [x.strip().lower().replace(' ', '_') for x in df.columns]

        df = df[COLS_SUBSET].copy()

        df['entidad_res'] = df['entidad_res'].astype(str).str.zfill(2)
        df['municipio_res'] = df['municipio_res'].astype(str).str.zfill(3)
        df['patient_residence_mun_id'] = df['entidad_res'] + df['municipio_res']
        df['patient_residence_mun_id'] = df['patient_residence_mun_id'].astype(int)
        df.drop(columns=['entidad_res', 'municipio_res', 'id_registro'], inplace=True)

        for col in ['fecha_actualizacion', 'fecha_ingreso', 'fecha_sintomas', 'fecha_def']:
            df[col] = df[col].str.replace('-', '').astype(int)

        df.rename(columns=rename_columns, inplace=True)

        # fix max death date
        max_current_date = df['updated_date'].max()
        df.loc[df['death_date'] > max_current_date, 'death_date'] = np.nan
        df['is_dead'] = 1
        df.loc[df['death_date'].isna(), 'is_dead'] = 0

        df['country_nationality_old'] = df['country_nationality'].astype(str)
        df['country_origin_old'] = df['country_origin'].astype(str)

        for col in ['country_origin', 'country_nationality']:
            df[col] = df[col].fillna('xxa').astype(str)
            df[col] = df[col].str.strip().str.lower()
            df[col] = df[col].apply(lambda x: norm(x))

        df['country_nationality'] = df['country_nationality'].map(RENAME_COUNTRIES)
        df['country_origin'] = df['country_origin'].map(RENAME_COUNTRIES)
        df['country_origin'] = df['country_origin'].fillna('xxa')
        df['country_nationality'] = df['country_nationality'].fillna('xxa')
        # assert df['country_nationality'].isnull().sum() == 0, "Null Countries (Nationality)"
        # assert df['country_origin'].isnull().sum() == 0, "Null Countries (Origin)"

        df['death_date'] = df['death_date'].astype(float)

        INT_LIST = ['updated_date', 'origin', 'type_health_institution_attended', 'health_institution_attended_ent',
                    'sex', 'patient_origin_ent_id', 'patient_type', 'ingress_date', 'symptoms_date', 'intubated',
                    'pneumonia_diagnose', 'age', 'nationality', 'pregnancy', 'speaks_indigenous_language', 'diabetes_diagnose',
                    'COPD_diagnose', 'asthma_diagnose', 'inmunosupresion_diagnose', 'hypertension_diagnose',
                    'diagnosis_another_disease', 'cardiovascular_diagnose', 'obesity_diagnose', 'chronic_kidney_failure_diagnose',
                    'smoking_diagnose', 'contact_another_covid_case', 'covid_positive', 'migrant', 'required_ICU',
                    'patient_residence_mun_id', 'is_dead']
        df[INT_LIST] = df[INT_LIST].astype(int)

        df['sex'] = df['sex'].astype(str)
        df['sex'].replace({'1': 2,
                        '2': 1}, inplace=True)

        # replace unknown municipalities
        df.loc[df['patient_residence_mun_id'].isin([97997, 98998, 99999]), 'patient_residence_mun_id'] = 33000

        # time.latest util
        df['time_id'] = df['updated_date']

        # ids refactor
        df['covid_positive'] = None
        df.loc[df['clasificacion_final'].isin([1,2,3]), 'covid_positive'] = 1
        df.loc[df['clasificacion_final'].isin([4,5,6]), 'covid_positive'] = 3
        df.loc[df['clasificacion_final'] == 7, 'covid_positive'] = 2

        # n of cases
        df['id'] = 1

        return df

class CovidPipeline(EasyPipeline):
    @staticmethod
    def steps(params):
        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))

        dtypes = {
            'time_id':                          'UInt32',
            'updated_date':                     'UInt32',
            'origin':                           'UInt8',
            'type_health_institution_attended': 'UInt8',
            'health_institution_attended_ent':  'UInt8',
            'sex':                              'UInt8',
            'patient_origin_ent_id':            'UInt8',
            'patient_type':                     'UInt8',
            'ingress_date':                     'UInt32',
            'symptoms_date':                    'UInt32',
            'death_date':                       'UInt32',
            'intubated':                        'UInt8',
            'pneumonia_diagnose':               'UInt8',
            'age':                              'UInt8',
            'nationality':                      'UInt8',
            'pregnancy':                        'UInt8',
            'speaks_indigenous_language':       'UInt8',
            'diabetes_diagnose':                'UInt8',
            'COPD_diagnose':                    'UInt8',
            'asthma_diagnose':                  'UInt8',
            'inmunosupresion_diagnose':         'UInt8',
            'hypertension_diagnose':            'UInt8',
            'diagnosis_another_disease':        'UInt8',
            'cardiovascular_diagnose':          'UInt8',
            'obesity_diagnose':                 'UInt8',
            'chronic_kidney_failure_diagnose':  'UInt8',
            'smoking_diagnose':                 'UInt8',
            'contact_another_covid_case':       'UInt8',
            'covid_positive':                   'UInt8',
            'migrant':                          'UInt8',
            'country_nationality':              'String',
            'country_origin':                   'String',
            'required_ICU':                     'UInt8',
            'is_dead':                          'UInt8',
            'patient_residence_mun_id':         'UInt16'
        }

        xform_step = TransformStep(connector=db_connector)
        load_step = LoadStep(
            'gobmx_covid', db_connector, if_exists='append', pk=['updated_date', 'time_id', 'symptoms_date', 'ingress_date', 
                            'patient_residence_mun_id', 'patient_origin_ent_id'], 
                            nullable_list=['death_date', 'country_nationality', 'country_origin'], dtype=dtypes
        )

        return [xform_step, load_step]

class CovidDownload(EasyPipeline):
    @staticmethod
    def steps(params):
        download_step = DownloadStep(
            connector='covid-data-mx',
            connector_path='conns.yaml',
            force=True
        )
        path = grab_parent_dir('.') + '/covid/'
        unzip_step = UnzipToFolderStep(compression='zip', target_folder_path=path)

        return [download_step, unzip_step]

if __name__ == '__main__':
    start_time = datetime.now()

    # download latest data
    pp = CovidDownload()
    while True:
        try:
            pp.run({})
            break

        except zipfile.BadZipFile:
            # catch error retry download
            print('Bad zip file error.')
            time.sleep(5)

    # drop table
    clean_tables('gobmx_covid')

    # ingest data
    pp = CovidPipeline()
    data = sorted(glob.glob('*.csv'))
    for chunk in pd.read_csv(data[-1], encoding='latin-1', iterator=True, chunksize=10**5):
        pp.run({
            'chunk': chunk
        })

    # update time dimension
    db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))
    min_query = 'SELECT min(ingress_date), min(symptoms_date), min(updated_date) FROM gobmx_covid'
    max_query = 'SELECT max(ingress_date), max(symptoms_date), max(updated_date) FROM gobmx_covid'
    min_value = str(min(query_to_df(db_connector, raw_query=min_query).iloc[0].to_list()))
    max_value = str(max(query_to_df(db_connector, raw_query=max_query).iloc[0].to_list()))
    min_time = '{}-{}-{}'.format(min_value[:4], min_value[4:6], min_value[6:])
    max_time = '{}-{}-{}'.format(max_value[:4], max_value[4:6], max_value[6:])

    pp = DimTimeDatePipeline()
    pp.run({'init': min_time,
            'end': max_time})

    # covid stats dim time table
    pp = DimTimeDateCovidStatsPipeline()
    pp.run({'init': min_time,
            'end': max_time})
    print('Duration: {}'.format(datetime.now() - start_time))