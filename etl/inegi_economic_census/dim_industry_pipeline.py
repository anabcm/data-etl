def split_code(df):
    df.code = df.code.astype('str')
    temp = df.loc[df.code.str.contains('-')].to_dict(orient='records')

    new_records = []
    for record in temp:
        #create range
        ran = []
        for n in record['code'].split('-'):
            ran.append(int(n))

        for n in range(ran[0],ran[-1]+1):
            add = {
                'code': str(n),
                'title': record['title']
            }
            new_records.append(add)

    df_temp = pd.DataFrame(new_records, columns=['code', 'title'])

    return df_temp


import pandas as pd
from bamboo_lib.models import PipelineStep, AdvancedPipelineExecutor
from bamboo_lib.models import Parameter, BasePipeline
from bamboo_lib.connectors.models import Connector
from bamboo_lib.steps import LoadStep
from bamboo_lib.helpers import grab_connector

class ReadStep(PipelineStep):
    # read data
    def run_step(self, prev, params):
        url = 'https://storage.googleapis.com/datamexico-data/inegi_economic_census/NAICS2017.xlsx'
        df_us = pd.read_excel(url, dtype='str')

        url = 'https://storage.googleapis.com/datamexico-data/inegi_economic_census/SCIAN2018.xlsx'
        df_mx = pd.read_excel(url, header=1, dtype='str')
        return df_mx, df_us

class DropFirstColStep(PipelineStep):
    def run_step(self, prev, params):
        df_mx, df_us = prev[0], prev[1]
        df_us.drop(columns=df_us.columns[0], inplace=True)
        return df_mx, df_us

class SelectStep(PipelineStep):
    def run_step(self, prev, params):
        df_mx, df_us = prev[0], prev[1]
        df_mx = df_mx[[df_mx.columns[0], df_mx.columns[1]]]
        df_us = df_us[[df_us.columns[0], df_us.columns[1]]]
        return df_mx, df_us

class RenameStep(PipelineStep):
    # format columns
    def run_step(self, prev, params):
        df_mx, df_us = prev[0], prev[1]
        df_mx.columns = ['code', 'title']
        df_us.columns = ['code', 'title']
        return df_mx, df_us

class DropNaStep(PipelineStep):
    # drop nan
    def run_step(self, prev, params):
        df_mx, df_us = prev[0], prev[1]
        for df in [df_mx, df_us]:
            df.dropna(axis=0, inplace=True, how='all')
            df.dropna(how='any', subset=[df.columns[0]], inplace=True)
        return df_mx, df_us

class CleanStep(PipelineStep):
    def run_step(self, prev, params):
        df_mx, df_us = prev[0], prev[1]
        df_mx.code = df_mx.code.str.strip()
        df_us.code = df_us.code.str.strip()
        df_mx.title = df_mx.title.str.strip()
        df_us.title = df_us.title.str.strip()
        return df_mx, df_us

class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        df_mx, df_us = prev[0], prev[1]
        # more than one sector
        df_mx = df_mx.append(split_code(df_mx))
        df_us = df_us.append(split_code(df_us))
        return df_mx, df_us

class SpecialCharacterStep(PipelineStep):
    def run_step(self, prev, params):
        df_mx, df_us = prev[0], prev[1]
        # character
        temp = list(df_mx.loc[df_mx.title.str.contains('T'), 'title'].unique())

        for ele in temp:
            if ele[-1] == 'T':
                df_mx.title.replace(ele, ele[:-1], inplace=True)
        return df_mx, df_us

class JoinStep(PipelineStep):
    def run_step(self, prev, params):
        df_mx, df_us = prev[0], prev[1]
        data = list(df_mx.loc[df_mx.code.str.len() == 6, 'code']) + list(df_us.loc[df_us.code.str.len() == 6, 'code'])
        df = pd.DataFrame(data, columns=['code'])
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
        
        return df, df_mx, df_us

class ReplaceStep(PipelineStep):
    def run_step(self, prev, params):
        df, df_mx, df_us = prev[0], prev[1], prev[2]
        
        df.sector_es.replace(list(df_mx.loc[df_mx.code.str.len() == 2, 'code']), list(df_mx.loc[df_mx.code.str.len() == 2, 'title']), inplace=True)
        df.subsector_es.replace(list(df_mx.loc[df_mx.code.str.len() == 3, 'code']), list(df_mx.loc[df_mx.code.str.len() == 3, 'title']), inplace=True)
        df.branch_es.replace(list(df_mx.loc[df_mx.code.str.len() == 4, 'code']), list(df_mx.loc[df_mx.code.str.len() == 4, 'title']), inplace=True)
        df.subbranch_es.replace(list(df_mx.loc[df_mx.code.str.len() == 5, 'code']), list(df_mx.loc[df_mx.code.str.len() == 5, 'title']), inplace=True)
        df.class_es.replace(list(df_mx.loc[df_mx.code.str.len() == 6, 'code']), list(df_mx.loc[df_mx.code.str.len() == 6, 'title']), inplace=True)

        df.sector_en.replace(list(df_us.loc[df_us.code.str.len() == 2, 'code']), list(df_us.loc[df_us.code.str.len() == 2, 'title']), inplace=True)
        df.subsector_en.replace(list(df_us.loc[df_us.code.str.len() == 3, 'code']), list(df_us.loc[df_us.code.str.len() == 3, 'title']), inplace=True)
        df.branch_en.replace(list(df_us.loc[df_us.code.str.len() == 4, 'code']), list(df_us.loc[df_us.code.str.len() == 4, 'title']), inplace=True)
        df.subbranch_en.replace(list(df_us.loc[df_us.code.str.len() == 5, 'code']), list(df_us.loc[df_us.code.str.len() == 5, 'title']), inplace=True)
        df.class_en.replace(list(df_us.loc[df_us.code.str.len() == 6, 'code']), list(df_us.loc[df_us.code.str.len() == 6, 'title']), inplace=True)

        return df


class DropDuplicatesStep(PipelineStep):
    def run_step(self, prev, params):
        df = prev
        df.drop_duplicates(inplace=True)
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
        step0 = ReadStep()
        step1 = DropFirstColStep()
        step2 = SelectStep()
        step3 = RenameStep()
        step4 = DropNaStep()
        step5 = CleanStep()
        step6 = TransformStep()
        step7 = SpecialCharacterStep()
        step8 = JoinStep()
        step9 = ReplaceStep()
        step10 = DropDuplicatesStep()
        # 'temp' == nombre de la tabla
        # 'coverage_schema' == nombre del esquema
        step11 = LoadStep("temp", postgres_connector, if_exists="replace", schema = "coverage_schema")

        # Definition of the pipeline and its steps
        pipeline = AdvancedPipelineExecutor(params)
        pipeline = pipeline.next(step0).next(step1).next(step2).next(step3).next(step4).next(step5).next(step6).next(step7).next(step8).next(step9).next(step10).next(step11)
        
        return pipeline.run_pipeline()


def run_coverage(params, **kwargs):
    pipeline = CoveragePipeline()
    pipeline.run(params)


if __name__ == '__main__':
    run_coverage({
        "database-connector": "postgres"
    })