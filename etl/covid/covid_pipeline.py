
import os
import glob
import numpy as np
import pandas as pd
from bamboo_lib.helpers import grab_parent_dir
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, PipelineStep, Parameter
from bamboo_lib.steps import DownloadStep, LoadStep, UnzipToFolderStep
from shared import rename_columns, rename_countries, values_check, NoUpdateException
from helpers import norm


class TransformStep(PipelineStep):
    def run_step(self, prev, params):

        data = sorted(glob.glob('*.csv'))

        if params.get('file_path'):
            df = pd.read_csv(params.get('file_path'), encoding='latin-1')
        else:
            df = pd.read_csv(data[-1], encoding='latin-1')

        df.columns = [x.strip().lower().replace(' ', '_') for x in df.columns]

        df['entidad_res'] = df['entidad_res'].astype(str).str.zfill(2)
        df['municipio_res'] = df['municipio_res'].astype(str).str.zfill(3)
        df['patient_residence_mun_id'] = df['entidad_res'] + df['municipio_res']
        df['patient_residence_mun_id'] = df['patient_residence_mun_id'].astype(int)
        df.drop(columns=['entidad_res', 'municipio_res', 'id_registro'], inplace=True)

        for col in ['fecha_actualizacion', 'fecha_ingreso', 'fecha_sintomas', 'fecha_def']:
            df[col] = df[col].str.replace('-', '').astype(int)

        df.rename(columns=rename_columns, inplace=True)
        df['death_date'].replace(99999999, np.nan, inplace=True)
        df['is_dead'] = 1
        df.loc[df['death_date'].isna(), 'is_dead'] = 0

        for col in ['country_origin', 'country_nationality']:
            df[col] = df[col].fillna('xxa')
            df[col] = df[col].str.strip().str.lower()
            df[col] = df[col].apply(lambda x: norm(x))

        df['country_nationality'].replace(rename_countries, inplace=True)
        df['country_origin'].replace(rename_countries, inplace=True)

        df['death_date'] = df['death_date'].astype(float)

        for col in [x for x in df.columns if x not in ['country_nationality', 'country_origin', 'death_date']]:
            df[col] = df[col].astype(int)

        df['id'] = range(1, df.shape[0]+1)

        df['sex'] = df['sex'].astype(str)
        df['sex'].replace({'1': 2,
                           '2': 1}, inplace=True)

        # replace unknown municipalities
        df.loc[df['patient_residence_mun_id'].isin([97997, 98998, 99999]), 'patient_residence_mun_id'] = 33000

        if values_check(df['updated_date'].max()):
            pass
        else:
            raise NoUpdateException

        return df

class CovidPipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return [
            Parameter(label='file_path', name='file_path', dtype=str)
        ]

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))

        dtypes = {
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

        download_step = DownloadStep(
            connector='covid-data-mx',
            connector_path='conns.yaml',
            force=True
        )

        path = grab_parent_dir('.') + '/covid/'
        unzip_step = UnzipToFolderStep(compression='zip', target_folder_path=path)
        xform_step = TransformStep(connector=db_connector)
        load_step = LoadStep(
            'gobmx_covid', db_connector, if_exists='append', pk=['id', 'updated_date', 'symptoms_date', 'ingress_date', 
                            'patient_residence_mun_id', 'patient_origin_ent_id', 
                            'country_nationality', 'country_origin'], nullable_list=['death_date'], dtype=dtypes
        )

        if params.get('file_path'):
            return [xform_step, load_step]
        else:
            return [download_step, unzip_step, xform_step, load_step]

if __name__ == '__main__':
    pp = CovidPipeline()
    pp.run({})