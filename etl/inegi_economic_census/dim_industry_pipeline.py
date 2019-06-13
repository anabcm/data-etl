import pandas as pd
from bamboo_lib.models import PipelineStep, AdvancedPipelineExecutor
from bamboo_lib.models import Parameter, BasePipeline
from bamboo_lib.connectors.models import Connector
from bamboo_lib.steps import LoadStep
from bamboo_lib.helpers import grab_connector

class OneStep(PipelineStep):
    # read data
    def run_step(self, prev, params):

        url = 'https://storage.googleapis.com/datamexico-data/inegi_economic_census/NAICS2017.xlsx'
        df_en = pd.read_excel(url)

        df_en.dropna(axis=0, inplace=True, how='all')
        df_en.dropna(how='any', subset=[df_en.columns[1]], inplace=True)

        df_en.drop(columns=df_en.columns[0], inplace=True)
        df_en = df_en[[df_en.columns[0], df_en.columns[1]]]
        df_en.columns = ['code', 'title']
        df_en.code = df_en.code.astype('str')

        # more than one sector
        temp = df_en.loc[df_en.code.str.contains('-')].to_dict(orient='records')

        new_records = []
        for record in temp:
            #create range
            ran = []
            for n in record['code'].split('-'):
                ran.append(int(n))

            for n in range(ran[0],ran[-1]+1):
                add = {
                    'code': n,
                    'title': record['title']
                }
                new_records.append(add)

        df_temp = pd.DataFrame(new_records, columns=['code', 'title'])

        df_en = df_en.append(df_temp)

        df_en.code = df_en.code.astype('str')

        df_en.title = df_en.title.str.strip()

        url = 'https://storage.googleapis.com/datamexico-data/inegi_economic_census/SCIAN2018.xlsx'
        df_mx = pd.read_excel(url, header=1)

        df_mx.dropna(axis=0, how='all', inplace=True)
        df_mx.dropna(how='any', subset=['Código'],  inplace=True)

        df_mx = df_mx[['Código', 'Título']]

        df_mx.columns = ['code', 'title']

        df_mx.code = df_mx.code.astype('str')

        df_mx.title = df_mx.title.str.strip()

        # more than one sector
        temp = df_mx.loc[df_mx.code.str.contains('-')].to_dict(orient='records')

        new_records = []
        for record in temp:
            #create range
            ran = []
            for n in record['code'].split('-'):
                ran.append(int(n))

            for n in range(ran[0],ran[-1]+1):
                add = {
                    'code': n,
                    'title': record['title']
                }
                new_records.append(add)

        df_temp = pd.DataFrame(new_records, columns=['code', 'title'])

        df_mx = df_mx.append(df_temp)

        df_mx.code = df_mx.code.astype('str')
        df_mx.code = df_mx.code.str.strip()
        df_en.code = df_en.code.astype('str')
        df_en.code = df_en.code.str.strip()

        # character
        temp = list(df_mx.loc[df_mx.title.str.contains('T'), 'title'].unique())

        for ele in temp:
            if ele[-1] == 'T':
                df_mx.title.replace(ele, ele[:-1], inplace=True)

        data = list(df_mx.loc[df_mx.code.str.len() == 6, 'code']) + list(df_en.loc[df_en.code.str.len() == 6, 'code'])
        df = pd.DataFrame(data, columns=['code'])
        df.code = df.code.astype('str')
        df.code = df.code.str.strip()

        df['sector_es'] = df.code.str[:2]
        df['subsector_es'] = df.code.str[:3]
        df['branch_es'] = df.code.str[:4]
        df['subbranch_es'] = df.code.str[:5]
        df['class_es'] = df.code.str[:]
        df['sector_en'] = df.code.str[:2]
        df['subsector_en'] = df.code.str[:3]
        df['branch_en'] = df.code.str[:4]
        df['subbranch_en'] = df.code.str[:5]
        df['class_en'] = df.code.str[:]

        df.sector_es.replace(list(df_mx.loc[df_mx.code.str.len() == 2, 'code']), list(df_mx.loc[df_mx.code.str.len() == 2, 'title']), inplace=True)
        df.subsector_es.replace(list(df_mx.loc[df_mx.code.str.len() == 3, 'code']), list(df_mx.loc[df_mx.code.str.len() == 3, 'title']), inplace=True)
        df.branch_es.replace(list(df_mx.loc[df_mx.code.str.len() == 4, 'code']), list(df_mx.loc[df_mx.code.str.len() == 4, 'title']), inplace=True)
        df.subbranch_es.replace(list(df_mx.loc[df_mx.code.str.len() == 5, 'code']), list(df_mx.loc[df_mx.code.str.len() == 5, 'title']), inplace=True)
        df.class_es.replace(list(df_mx.loc[df_mx.code.str.len() == 6, 'code']), list(df_mx.loc[df_mx.code.str.len() == 6, 'title']), inplace=True)

        df.sector_en.replace(list(df_en.loc[df_en.code.str.len() == 2, 'code']), list(df_en.loc[df_en.code.str.len() == 2, 'title']), inplace=True)
        df.subsector_en.replace(list(df_en.loc[df_en.code.str.len() == 3, 'code']), list(df_en.loc[df_en.code.str.len() == 3, 'title']), inplace=True)
        df.branch_en.replace(list(df_en.loc[df_en.code.str.len() == 4, 'code']), list(df_en.loc[df_en.code.str.len() == 4, 'title']), inplace=True)
        df.subbranch_en.replace(list(df_en.loc[df_en.code.str.len() == 5, 'code']), list(df_en.loc[df_en.code.str.len() == 5, 'title']), inplace=True)
        df.class_en.replace(list(df_en.loc[df_en.code.str.len() == 6, 'code']), list(df_en.loc[df_en.code.str.len() == 6, 'title']), inplace=True)
        df.drop_duplicates(inplace=True)
        
        return df


class RenameStep(PipelineStep):
    # format columns
    def run_step(self, prev, params):
        df = prev
        df.columns = ['estado_federativo', 'tipo_limitacion', 'poblacion_limitacion_actividad', 'nacimiento', 'enfermedad', 'accidente', 'edad_avanzada', 'otra_causa', 'no_especificado']
        return df


class DropNaStep(PipelineStep):
    # drop nan
    def run_step(self, prev, params):
        df = prev
        df.dropna(axis=0, inplace=True, subset=['tipo_limitacion', 'poblacion_limitacion_actividad'])
        return df

class DropValuesStep(PipelineStep):
    # drop values
    def run_step(self, prev, params):
        df = prev
        df.drop(df[df.tipo_limitacion == "Total"].index , inplace=True)
        df.drop(columns=['poblacion_limitacion_actividad'], inplace=True)
        return df

class CoveragePipeline(BasePipeline):
    @staticmethod
    def pipeline_id():
        return 'program-coverage-pipeline-temp'

    @staticmethod
    def name():
        return 'Program Coverage Pipeline temp'

    @staticmethod
    def description():
        return 'Processes information from Mexico'

    @staticmethod
    def website():
        return 'http://datawheel.us'

    @staticmethod
    def parameter_list():
        return [
            Parameter(label="Source connector", name="source-connector", dtype=str, source=Connector)
        ]

    @staticmethod
    def run(params, **kwargs):
        # Use of connectors specified in the conns.yaml file
        postgres_connector = grab_connector(__file__, params.get("database-connector"))

        # Definition of each step
        step1 = OneStep()
        # 'temp' == nombre de la tabla
        # 'coverage_schema' == nombre del esquema
        step2 = LoadStep("temp", postgres_connector, if_exists="replace", schema = "coverage_schema")

        # Definition of the pipeline and its steps
        pipeline = AdvancedPipelineExecutor(params)
        pipeline = pipeline.next(step1).next(step2)
        return pipeline.run_pipeline()


def run_coverage(params, **kwargs):
    pipeline = CoveragePipeline()
    pipeline.run(params)


if __name__ == '__main__':
    run_coverage({
        "database-connector": "postgres"
    })