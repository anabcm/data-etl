import pandas as pd
from bamboo_lib.models import PipelineStep, AdvancedPipelineExecutor
from bamboo_lib.models import Parameter, EasyPipeline
from bamboo_lib.connectors.models import Connector
from bamboo_lib.steps import LoadStep

class CreateStep(PipelineStep):
    def run_step(self, prev, params):
        start='1990-01-01'
        end='2080-12-31'
        df = pd.DataFrame({'date': pd.date_range(start, end)})
        df['week_day'] = df.date.dt.weekday_name
        df['day'] = df.date.dt.day
        df['month'] = df.date.dt.day

        df['week'] = df.date.dt.weekofyear
        df['quarter'] = df.date.dt.quarter
        df['year'] = df.date.dt.year

        df.insert(0, 'date_id', (df.date.dt.year.astype(str) + df.date.dt.month.astype(str).str.zfill(2) + df.date.dt.day.astype(str).str.zfill(2)).astype(int))
        df['date'] = pd.to_datetime(df['date']).dt.date

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
    def parameter_list():
        return [
            Parameter(label='Source connector', name='source-connector', dtype=str, source=Connector)
        ]

    @staticmethod
    def steps(params):
        # Use of connectors specified in the conns.yaml file
        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))

        dtypes = {
            'date_id': 'UInt32',
            'date': 'Date'
        }

        # Definition of each step
        create_step = CreateStep()
        load_step = LoadStep('dim_date', db_connector, if_exists='drop', pk=['date_id'], dtype=dtypes)
        
        return [create_step, load_step]