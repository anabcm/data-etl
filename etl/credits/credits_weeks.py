
import pandas as pd
from bamboo_lib.models import PipelineStep, EasyPipeline
from bamboo_lib.connectors.models import Connector
from bamboo_lib.steps import LoadStep
from shared import APPROVED_WEEK


class TransformStep(PipelineStep):
    def run_step(self, prev, params):

        df = pd.DataFrame.from_dict(APPROVED_WEEK, orient='index')
        df.reset_index(inplace=True)
        df.columns = ['approved_week_name', 'approved_week']

        return df

class CreditsWeeksPipeline(EasyPipeline):
    @staticmethod
    def steps(params, **kwargs):
        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))

        dtypes = {
            'approved_week':  'UInt32'
        }

        transform_step = TransformStep(connector=db_connector)
        load_step = LoadStep('credits_weeks', db_connector, dtype=dtypes,
                if_exists='drop', pk=['approved_week'])
        
        return [transform_step, load_step]

if __name__ == '__main__':
    pp = CreditsWeeksPipeline()
    pp.run({})