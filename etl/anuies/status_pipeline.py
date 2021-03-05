
import nltk
import pandas as pd
from etl.helpers import format_text
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, PipelineStep, Parameter
from bamboo_lib.steps import DownloadStep, LoadStep
from bamboo_lib.helpers import query_to_df
from static import ORIGIN, STAT, SEX, TYPES

class ReadStep(PipelineStep):
    def run_step(self, prev, params):
        # read data
        df = pd.read_excel(prev[0], header=1)
        df.columns = [x.lower().replace('sum of ', '') for x in df.columns]
        df.rename(columns={'entidad': 'ent_id', 'entidad federativa': 'ent_id', 'municipio': 'mun_id',
                           'cve campo unitario': 'career', 'clave campo unitario': 'career', 'nivel': 'type',
                           'nombre institución': 'institution_id', 'nombre institución anuies': 'institution_id',
                           'nombre escuela/campus anuies': 'campus_id', 'nombre escuela/campus/plantel': 'campus_id',
                           'nombre carrera sep': 'program', 'nombre programa educativo': 'program'}, inplace=True)
        return df, prev[1], prev[2], prev[3], prev[4]

class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        df, raw_to_careers, careers_to_id, raw_to_institution, institution_to_id = prev
        # type format
        for col in ['ent_id', 'mun_id', 'career', 'type', 'campus_id', 'institution_id', 'program']:
            df[col] = df[col].ffill()
        df.ent_id = df.ent_id.str.title()

        # ids replace from external table
        df.ent_id.replace(ORIGIN, inplace=True)

        # totals clean
        df.career = df.career.astype('str')
        for col in ['mun_id', 'career', 'type']:
            print('Current col:', col)
            df[col] = df[col].astype(str)
            df = df.loc[df[col].str.contains('Total') == False].copy()
            df[col] = df[col].str.strip().str.replace('  ', ' ').str.replace(':', '')
        df.career = df.career.str.replace('.', '').astype('int')
        df['program'] = df['program'].astype(str).str.strip().str.upper()

        # municipality level id
        try:
            df.loc[:, 'mun_id'] = df.loc[:, 'ent_id'].astype(float).astype(int).astype(str) + \
                    df.loc[:, 'mun_id'].astype(float).astype(int).astype(str).str.zfill(3)
        except Exception as e:
            print('Municipality level id:', e)
            query = 'SELECT mun_id, mun_name FROM dim_shared_geography_mun'
            geo_mun = query_to_df(self.connector, raw_query=query).drop_duplicates(subset=['mun_id'])
            geo_mun['mun_name'] = geo_mun['mun_name'].str.upper()
            df['mun_id'].replace(dict(zip(geo_mun['mun_name'], geo_mun['mun_id'])), inplace=True)
        df.drop(columns=['ent_id'], inplace=True)

        # column names format
        df.columns = df.columns.str.replace('suma de ', '')

        # melt step
        try:
            df = df[['mun_id', 'career', 'type', 'campus_id', 'institution_id', 'program', 'e-h', 'e-m', 'g-h', 'g-m']].copy()

        except:
            df = df[['mun_id', 'career', 'type', 'campus_id', 'institution_id', 'program', 'e-h', 'e-m', 't-h', 't-m']].copy()

        df = df.melt(id_vars=['mun_id', 'career', 'type', 'campus_id', 'institution_id', 'program'], var_name='stat', value_name='value')
        df = df.loc[df.value != 0]

        split = df['stat'].str.split('-', n=1, expand=True) 
        df['stat'] = split[0]
        df['sex'] = split[1]

        # encoding
        df.stat.replace(STAT, inplace=True)
        df.sex.replace(SEX, inplace=True)
        df.type.replace(TYPES, inplace=True)

        # 2020 version status fix
        df.loc[(df['type'].isin([12, 13, 14])) & (df['stat'] == 't'), 'stat'] = 'g'

        # stopwords es
        nltk.download('stopwords')

        # career_id replace
        # Raw rame -> processed name
        careers = pd.read_csv(raw_to_careers)
        df.loc[df['program'].isin(['#NAME?', '#¿NOMBRE?']), 'program'] = '0'
        df = df.loc[~df['program'].str.upper().str.contains('TOTAL')].copy()
        missing_ids = df.loc[(~df['program'].isin(list(careers['backup']) + ['0']))].shape[0]
        print('Missing ids (careers):', missing_ids)
        if missing_ids > 0:
            print(df.loc[~df['program'].isin(list(careers['backup']))])
            df.loc[~df['program'].isin(list(careers['backup']))].to_csv('review_career.csv', index=False)
        df.program = df.program.replace(dict(zip(careers.backup, careers.name_es)))
        # Processed name -> numerical id
        careers = pd.read_csv(careers_to_id)
        df.program = df.program.replace(dict(zip(careers.backup, careers.career_id)))

        # campus_id replace
        for col in ['institution_id', 'campus_id']:
            df[col] = df[col].astype(str).str.strip()
        df['backup'] = (df['institution_id'] + df['campus_id']).str.strip()

        # Raw rame -> processed name
        institution = pd.read_csv(raw_to_institution)
        df = df.loc[~df['backup'].str.upper().str.contains('TOTAL')].copy()
        missing_ids = df.loc[~df['backup'].isin(list(institution['backup']))].shape[0]
        print('Missing ids (institutions):', missing_ids)
        if missing_ids > 0:
            print(df.loc[~df['backup'].isin(list(institution['backup']))])
            df.loc[~df['backup'].isin(list(institution['backup']))].to_csv('review_campus.csv', index=False)
        df.backup = df.backup.replace(dict(zip(institution.backup, institution.backup_raw)))
        # Processed name -> numerical id
        institution = pd.read_csv(institution_to_id)
        df.backup = df.backup.replace(dict(zip(institution.backup_raw, institution.campus_id)))
        df['campus_id'] = df['backup']

        for col in ['mun_id', 'career', 'program', 'type', 'sex', 'value', 'stat', 'campus_id']:
            print('Current col:', col)
            df[col] = df[col].astype('float')

        df = df[['mun_id', 'campus_id', 'program', 'type', 'sex', 'value', 'stat']].copy()

        df['year'] = int(params.get('year_plus'))

        return df

class StatusPipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return [
            Parameter(name='dataset', dtype=str),
            Parameter(name='year', dtype=int),
            Parameter(name='year_plus', dtype=int),
            Parameter(name='custom', dtype=bool)
        ]

    @staticmethod
    def steps(params):

        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))

        dtype = {
            'mun_id':      'UInt16',
            'type':        'UInt8',
            'year':        'UInt16',
            'campus_id':   'UInt64',
            'program':     'UInt64',
            'stat':        'UInt8',
            'value':       'UInt32',
            'sex':         'UInt8'
        }

        if params.get('custom'):
            download_step = DownloadStep(
                connector=['data-2020', 'raw-career', 'dim-career', 'raw-institution', 'dim-institution'],
                connector_path='conns.yaml'
            )
        else:
            download_step = DownloadStep(
                connector=['data', 'raw-career', 'dim-career', 'raw-institution', 'dim-institution'],
                connector_path='conns.yaml'
            )

        read_step = ReadStep()
        transform_step = TransformStep(connector=db_connector)
        load_step = LoadStep('anuies_status', db_connector, if_exists='append', pk=['mun_id', 'campus_id', 'program', 'year'], dtype=dtype)

        return [download_step, read_step, transform_step, load_step]


if __name__ == "__main__":
    pp = StatusPipeline()
    for year in range(2016, 2019):
        for dataset in ['licenciatura', 'posgrado']: 
            pp.run({
                'dataset': dataset,
                'year': str(year),
                'year_plus': str(year+1),
                'custom': False
            })

    # 2020 version is one file only
    pp.run({
        'year_plus': '2020',
        'custom': True
    })