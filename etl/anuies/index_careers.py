import nltk
import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, PipelineStep, Parameter
from bamboo_lib.steps import LoadStep
from bamboo_lib.helpers import grab_connector
from helpers import format_text, create_index, query_to_df, gouped_index

class ReadStep(PipelineStep):
    def run_step(self, prev, params):
        # read data
        df = pd.read_excel(params.get('url'), dtypes='str', header=1, usecols=list(range(0,11)))
        df.columns = df.columns.str.lower()

        return df

class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        df = prev
        
        # subset
        columns = {
            'nombre carrera sep': 'name_es',
            'cve campo unitario': 'code',
        }
        df.rename(columns=columns, inplace=True)
        df = df[['code', 'name_es']].copy()
        
        # type conversion
        for col in ['code', 'name_es']:
            df = df.loc[df[col].astype('str').str.contains('Total') == False].copy()
        
        # delete totals
        df['code'] = df['code'].ffill().astype('int')
        df['name_es'] = df['name_es'].ffill().str.replace('  ', ' ').str.replace(':', '')

        # characters
        operations = {
            '“L2”': '"L2"',
            '–': '-'
        }
        for k, v in operations.items():
            df.name_es = df.name_es.str.replace(k, v)

        # careers ids
        nltk.download('stopwords')
        df = format_text(df, ['name_es'], stopwords=nltk.corpus.stopwords.words('spanish'))

        df.drop_duplicates(subset=['name_es'], inplace=True)

        # column creation
        df['area'] = df['code']
        
        # index creation
        try:
            db_connector = grab_connector("../conns.yaml", "clickhouse-database")
            query = "SELECT * from dim_shared_careers_anuies"
            temp = query_to_df(db_connector, query, table_name='dim_shared_careers_anuies')
            df = df.append(temp)
            df.drop_duplicates(subset=['name_es'], inplace=True)
        except Exception:
            print('Table does not exist.')
        
        # re:do column with correct area
        df['code'] = df['area'].astype('str').str.zfill(6)
        df.sort_values(by='code', inplace=True)
        
        ### index creation
        df = gouped_index(df, 'code')
        df.code = df.code + df.id.astype('str').str.zfill(4)
        df.drop(columns=['id'], inplace=True)
        df.code = df.code.astype('int64')
        
        return df

class CareersPipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return [
            Parameter(name='url', dtype=str)
        ]

    @staticmethod
    def steps(params):
        
        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))
        
        dtype = {
            'code':    'UInt32',
            'name_es': 'String',
            'area':    'UInt32'
        }
        
        read_step = ReadStep()
        transform_step = TransformStep()
        load_step = LoadStep('dim_shared_careers_anuies', db_connector, if_exists='drop', 
                            pk=['code'], dtype=dtype, engine='ReplacingMergeTree')

        return [read_step, transform_step, load_step]