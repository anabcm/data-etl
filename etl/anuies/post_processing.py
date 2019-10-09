def operation(series, target, inplace=False):
    # upper() fixed
    try:       
        unique_rows = series.loc[series.str.contains(target)].unique()
        for ele in unique_rows:
            val = ele.split()
            for v in range(len(val)):
                if val[v].lower() == target.lower():
                    val[v] = val[v].upper()
                    if inplace:
                        series.loc[series == ele] = ' '.join(val).strip()
                    else:
                        return ' '.join(val).strip()
    except:
        return 'Target: {} not found'.format(target)

import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.helpers import grab_connector
from bamboo_lib.models import EasyPipeline, PipelineStep, Parameter
from bamboo_lib.steps import LoadStep
from helpers import query_to_df

class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        # read data
        try:
            db_connector = grab_connector("../conns.yaml", "clickhouse-database")
            query = "SELECT * from dim_shared_work_centers"
            df = query_to_df(db_connector, query, table_name='dim_shared_work_centers')
        except:
            print('Table does not exist.')

        # replace
        replace = { 'Universidad Ucugs': 'Universidad CUGS', 
                    'Universidad de Cuautitlán I.': 'Universidad de Cuautitlán Izcalli',
                    'Universidad Anglohispanomexicana': 'Universidad AngloHispanoMexicana',
                    'U N a R T E': 'UNARTE',
                    'Universidad Unilider': 'Universidad UNILÍDER',
                    'Ceni Jur-Centro de Investigación Jurídica A. C.': 'CENIJUR Centro de Investigación Jurídica A. C.',
                    'Ep de México': 'EP de México',
                    'In-Q-Ba Formación de Emprendedores': 'in.Q.ba Formación de Emprendedores',
                    'Profa. Adela Márquez de Martinez': 'Profesora Adela Márquez de Martinez'}
        df.institution_name.replace(replace, inplace=True)

        campus = [' Ucugs ', ' Upn ', ' Dcm ']
        for ele in campus:
            df.campus_name = df.campus_name.str.replace(ele, ele.upper())
        df.campus_name = df.campus_name.str.replace(' UCUGS ', ' CUGS ')

        df.institution_name = df.institution_name.str.replace('U P N', 'UPN')
        df.institution_name = df.institution_name.str.replace('U.P.N', 'UPN')

        institutions = ['Cut', 'Pgjdf', 'Upn', 'Upn', 'Udlap', 'U.P.N', 'Cidh', 'Unir', 'Dcm', 
                     'Icon', 'Xxi', 'Inecuh', 'Etac', 'Uteg', 'Inst Nac', 'Une', 'Cade', 
                     'Cudec', 'Ralj', 'Cipae', 'Ceuni', 'Siati', 'Cup ', 'Am', 'Snte', 'Ives']

        for ele in institutions:
            if operation(df.institution_name, ele) == None:
                None
            else:
                operation(df.institution_name, ele, inplace=True)

        df.institution_name.loc[df.institution_name.str.contains('Inst Nac')] = 'Instituto Nacional de Ortodoncia y Ortopedia Maxilar'
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
        
        transform_step = TransformStep()
        load_step = LoadStep('dim_shared_work_centers', db_connector, if_exists='drop', 
                            pk=['institution_id', 'campus_id'], dtype=dtype, engine='ReplacingMergeTree')

        return [transform_step, load_step]