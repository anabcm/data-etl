import nltk
import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, PipelineStep, Parameter
from bamboo_lib.steps import LoadStep
from bamboo_lib.helpers import grab_connector
from helpers import format_text, create_index, query_to_df, word_case

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

        df = df.loc[df['campus_name'].str.contains('Total') == False].copy()
        df['campus_name'] = df['campus_name'].str.strip().str.replace('  ', ' ').str.replace(':', '')
        df['campus_id'] = df['campus_id'].str.strip()

        for ele in df['institution_name'].unique():
            if '-' in ele:
                df.loc[df['institution_name'] == ele, 'institution_name'] = ele.split(' - ')[0]

        nltk.download('stopwords')
        stopwords_es = nltk.corpus.stopwords.words('spanish')

        df = format_text(df, ['institution_name', 'campus_name'], stopwords=stopwords_es)

        operations = {
            '  ': ' ',
            ',': '',
            ')': '',
            '(': '',
            '“L2”': '"L2"',
            '–': '-',
            'U P N': 'UPN',
            'U.P.N.': 'UPN',
            'A. C.': 'A.C.',
            'S. C.': 'S.C.',
            'Centro A+ D': 'Centro A+D',
            'IfcpeS.C.': 'IFCP S.C.',
            'Unideal': 'Universidad de Altamira',
            'Ingenihum': 'IngeniHum',
        }
        for k, v in operations.items():
            df.institution_name = df.institution_name.str.replace(k, v)

        sost = {
            'PÚBLICO': 1,
            'PARTICULAR': 2
        }
        df.sostenimiento.replace(sost, inplace=True)

        # replace
        replace = { 'Universidad Ucugs': 'Universidad CUGS', 
                    'Universidad de Cuautitlán I.': 'Universidad de Cuautitlán Izcalli',
                    'Universidad Anglohispanomexicana': 'Universidad AngloHispanoMexicana',
                    'U N a R T E': 'UNARTE',
                    'Universidad Unilider': 'Universidad UNILÍDER',
                    'Ceni Jur-Centro de Investigación Jurídica A. C.': 'CENIJUR Centro de Investigación Jurídica A. C.',
                    'Ep de México': 'EP de México',
                    'In-Q-Ba Formación de Emprendedores': 'in.Q.ba Formación de Emprendedores',
                    'Profa. Adela Márquez de Martinez': 'Profesora Adela Márquez de Martinez',
                    'Instituto Henri Dumagt': 'Instituto Henri Dunant',
                    'Escuela de Terapias del Cree del Difem': 'Escuela de Terapias del CREE del DIFEM',
                    'Acai para la Formación y El Desarrollo': 'ACaI para la Formación y El Desarrollo',
                    'Instituto de Investigación Para las Ciencias Ambientales Ac': 'Instituto de Investigación Para las Ciencias Ambientales A.c.'}
        df.institution_name.replace(replace, inplace=True)

        campus = [' Ucugs ', ' Upn ', ' Dcm ']
        for ele in campus:
            df.campus_name = df.campus_name.str.replace(ele, ele.upper())
        df.campus_name = df.campus_name.str.replace(' UCUGS ', ' CUGS ')

        institutions = ['Cut', 'Crea', 'Ugc', 'Ssc', 'Oca', 'Pgjdf', 'Upn', 'Upn', 'Udlap', 'U.P.N', 'Cidh', 'Unir', 'Dcm', 'Iseti', 'Imei',
                     'Icon', 'Xxi', 'Inecuh', 'Etac', 'Uteg', 'Une', 'Cade', 'Cecomsi', 'Ises', 'Ifra', 'Utt', 'Itian', 'Ceickor', 'Cedva',
                     'Cescet', 'Cudec', 'Ralj', 'Cipae', 'Ceuni', 'Siati', 'Cup ', 'Am', 'Snte', 'Ives', 'Infocap', 'Ui', 'Ceval', 'Ii', 'Iap',
                     'Fstse', 'Sae', 'Iii', 'Cecyt', 'Ceunico', 'Ion', 'Cup', 'Udes', 'Arpac', 'Isic', 'Itec', 'Itca', 'Cife', 'Uniem', 'Asec',
                     'Ymca', 'Inace', 'Ort', 'Isec', 'Issste', 'Icel', 'Ilef', 'Ipn', 'Cnci', 'Univer', 'Part']

        for ele in institutions:
            if word_case(df.institution_name, ele) == None:
                None
            else:
                word_case(df.institution_name, ele, inplace=True)

        df.institution_name.loc[df.institution_name.str.contains('Inst Nac')] = 'Instituto Nacional de Ortodoncia y Ortopedia Maxilar'

        # comprobar existencia de datos
        df.drop_duplicates(subset=['campus_id'], inplace=True)
        try:
            db_connector = grab_connector("../conns.yaml", "clickhouse-database")
            query = "SELECT * from dim_shared_work_centers"
            temp = query_to_df(db_connector, query, table_name='dim_shared_work_centers')
            temp.drop(columns=['institution_id'], inplace=True)
            df = df.append(temp, sort=True).copy()
            df.drop_duplicates(subset=['campus_id'], inplace=True)
        except Exception as e:
            print(e)
            None
        print(df.shape)
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