
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

        df.drop(columns=['version', 'backup'], inplace=True)

        return df

class CareersPipeline(EasyPipeline):
    @staticmethod
    def steps(params):

        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))

        dtype = {
            'career_id':            'UInt64',
            'career_name_es':       'String',
            'career_name_en':       'String',
            'speciality_id':        'UInt32',
            'speciality_name_es':   'String',
            'speciality_name_en':   'String',
            'subfield_id':          'UInt16',
            'subfield_es':          'String',
            'subfield_en':          'String',
            'field_id':             'UInt8',
            'field_es':             'String',
            'field_en':             'String',
            'area_id':              'UInt8',
            'area_es':              'String',
            'area_en':              'String'
        }

        download_step = DownloadStep(
            connector='dim-career',
            connector_path='conns.yaml',
            force=True
        )

        read_step = ReadStep()
        transform_step = TransformStep()

        load_step = LoadStep('dim_anuies_careers', db_connector, if_exists='drop', pk=['career_id', 
                             'speciality_id', 'subfield_id', 'field_id', 'area_id'], dtype=dtype)

        return [download_step, read_step, transform_step, load_step]

if __name__ == "__main__":
    pp = CareersPipeline()
    pp.run({})