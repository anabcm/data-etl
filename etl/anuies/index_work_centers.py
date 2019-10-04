import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, PipelineStep, Parameter
from bamboo_lib.steps import LoadStep
from bamboo_lib.helpers import grab_connector
from helpers import format_text, create_index, query_to_df

class ReadStep(PipelineStep):
    def run_step(self, prev, params):
        # read data
        df = pd.read_excel(params.get('url'), dtypes='str', header=1)
        df.columns = df.columns.str.lower()

        return df

class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        df = prev
        columns = {
            'clave centro de trabajo': 'campus_id',
            'clave institución': 'institution_code',
            'nombre carrera sep': 'career_name',
            'nombre institución anuies': 'institution_name',
            'nombre escuela/campus anuies': 'campus_name',
            'cve campo unitario': 'career_code',
            'entidad': 'ent_id',
            'municipio': 'mun_id'
        }
        df.rename(columns=columns, inplace=True)

        df = df[['campus_id', 'institution_name', 'campus_name', 'sostenimiento']].copy()
        ### Clean step
        for col in df.columns:
            df[col] = df[col].ffill()

        df.drop_duplicates(subset=['campus_id'], inplace=True)

        df = df.loc[df['campus_name'].str.contains('Total') == False].copy()
        df['campus_name'] = df['campus_name'].str.strip().str.replace('  ', ' ').str.replace(':', '')

        for ele in df['institution_name'].unique():
            if '-' in ele:
                df.loc[df['institution_name'] == ele, 'institution_name'] = ele.split(' - ')[0]

        stopwords_es = ['a', 'e', 'en', 'ante', 'con', 'contra', 'de', 'del', 'desde', 'la', 'lo', 'las', 'los', 'y']

        df = format_text(df, ['institution_name', 'campus_name'], stopwords=stopwords_es)

        operations = {
            '  ': ' ',
            ',': ''
        }
        for k, v in operations.items():
            df.institution_name = df.institution_name.str.replace(k, v)

        sost = {
            'PÚBLICO': 1,
            'PARTICULAR': 2
        }
        df.sostenimiento.replace(sost, inplace=True)
        
        # comprobar existencia de datos
        try:
            db_connector = grab_connector("../conns.yaml", "clickhouse-database")
            query = "SELECT * from dim_shared_work_centers"
            temp = query_to_df(db_connector, query, table_name='dim_shared_work_centers')
            temp.drop(columns=['institution_id'], inplace=True)
            if temp.shape[0] > 0:
                df = df.append(temp)
                df.drop_duplicates(subset=['campus_id'], inplace=True)
        except:
            None

        ### institution id
        df = create_index(df, 'institution_name', 'institution_id').copy()

        df['sostenimiento'] = df['sostenimiento'].astype('int')
        
        return df

class WorkCentersPipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return [
            Parameter(name='url', dtype=str)
        ]

    @staticmethod
    def steps(params):
        
        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))
        
        dtype = {
            'campus_id':       'String',
            'campus_name':     'String',
            'institution_name':'String',
            'institution_id':  'UInt16',
            'sostenimiento':   'UInt8'
        }
        
        read_step = ReadStep()
        transform_step = TransformStep()
        load_step = LoadStep('dim_shared_work_centers', db_connector, if_exists='drop', 
                            pk=['institution_id', 'campus_id'], dtype=dtype, engine='ReplacingMergeTree')

        return [read_step, transform_step, load_step]