
import pandas as pd
from bamboo_lib.models import PipelineStep, EasyPipeline
from bamboo_lib.connectors.models import Connector
from bamboo_lib.steps import DownloadStep, LoadStep


class TransformStep(PipelineStep):
    def run_step(self, prev, params):

        df = pd.read_csv(prev)

        return df

class CreditsWeeksPipeline(EasyPipeline):
    @staticmethod
    def steps(params, **kwargs):
        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))

        dtypes = {
            'approved_week':  'UInt32'
        }

        download_step = DownloadStep(
            connector='dim-weeks',
            connector_path='conns.yaml',
            force=True
        )

        transform_step = TransformStep(connector=db_connector)
        load_step = LoadStep('credits_weeks', db_connector, dtype=dtypes,
                if_exists='drop', pk=['approved_week'])
        
        return [download_step, transform_step, load_step]

if __name__ == '__main__':
    pp = CreditsWeeksPipeline()
    pp.run({})