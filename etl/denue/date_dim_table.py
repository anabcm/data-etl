import pandas as pd
from bamboo_lib.models import PipelineStep, AdvancedPipelineExecutor
from bamboo_lib.models import Parameter, EasyPipeline
from bamboo_lib.connectors.models import Connector
from bamboo_lib.steps import LoadStep

class CreateStep(PipelineStep):
    def run_step(self, prev, params):

        start='1990-01-01'
        end='2030-12-31'
        df = pd.DataFrame({'date': pd.date_range(start, end)})
        df['month'] = df.date.dt.month
        df['quarter'] = df.date.dt.quarter
        df['year'] = df.date.dt.year

        df.insert(0, 'date_id', (df.date.dt.year.astype(str) + df.date.dt.month.astype(str).str.zfill(2)))
        df.drop(columns='date', inplace=True)

        df = df.groupby(['date_id', 'month', 'quarter', 'year']).sum().reset_index(col_fill='ffill')

        return df

class CoveragePipeline(EasyPipeline):
    @staticmethod
    def pipeline_id():
        return 'program-coverage-pipeline'

    @staticmethod
    def name():
        return 'Program Coverage Pipeline'

    @staticmethod
    def description():
        return 'Create Date Dimension Table'

    @staticmethod
    def website():
        return 'http://datawheel.us'

    @staticmethod
    def steps(params):
        # Use of connectors specified in the conns.yaml file
        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))

        dtypes = {
            'date_id': 'UInt32',
        }

        # Definition of each step
        create_step = CreateStep()
        load_step = LoadStep('dim_date_month', db_connector, if_exists='drop', pk=['date_id'], dtype=dtypes)
        
        return [create_step, load_step]