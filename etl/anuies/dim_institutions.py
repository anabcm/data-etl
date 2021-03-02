
import pandas as pd
from bamboo_lib.models import EasyPipeline, PipelineStep
from bamboo_lib.connectors.models import Connector
from bamboo_lib.steps import LoadStep, DownloadStep

class ReadStep(PipelineStep):
    def run_step(self, prev, params):
        df = pd.read_csv(prev)
        return df

class TransformStep(PipelineStep):
    def run_step(self, prev, params):

        df = prev

        df.drop(columns=['backup_raw', 'backup'], inplace=True)

        return df

class InstitutionsPipeline(EasyPipeline):
    @staticmethod
    def steps(params):

        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))

        dtype = {
            'campus_id':            'UInt64',
            'campus_name':          'String',
            'institution_id':       'UInt32',
            'institution_name':     'String'
        }

        download_step = DownloadStep(
            connector='dim-institution',
            connector_path='conns.yaml',
            force=True
        )

        read_step = ReadStep()
        transform_step = TransformStep()

        load_step = LoadStep('dim_anuies_institutions', db_connector, 
                    if_exists='drop', pk=['campus_id', 'institution_id'], dtype=dtype)

        return [download_step, read_step, transform_step, load_step]

if __name__ == "__main__":
    pp = InstitutionsPipeline()
    pp.run({})