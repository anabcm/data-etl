
import pandas as pd
from bamboo_lib.models import PipelineStep, AdvancedPipelineExecutor
from bamboo_lib.models import Parameter, BasePipeline
from bamboo_lib.connectors.models import Connector
from bamboo_lib.steps import LoadStep

class ReadStep(PipelineStep):
    def run_step(self, prev, params):
        # foreign trade data
        url = 'https://storage.googleapis.com/datamexico-data/foreign_trade/Commercial_data_mx.csv'
        df = pd.read_csv(url, encoding='latin-1', dtype='object', header=0)
        return df

class CleanStep(PipelineStep):
    def run_step(self, prev, params):
        df = prev
        df.drop(0, inplace=True)
        df.drop(columns=['EN'], inplace=True)
        df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(', '').str.replace(')', '').str.replace('__', '_')
        return df

class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        df = prev
        df.date_scale = pd.to_datetime(df.date_scale).dt.date
        df['id'] = range(df.shape[0])
        for col in ['unitary_price', 'commercial_value', 'code_of_dispatch_customs_section', 'sequence_of__tariff_fractions', 'code_of_entry_customs_section', 'tariff_fraction', 'postal_code_taxpayer_address', 'commercial_measure_code']:
            df[col] = df[col].astype('float')
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
            Parameter(label='Source connector', name='source-connector', dtype=str, source=Connector)
        ]

    @staticmethod
    def run(params, **kwargs):
        # Use of connectors specified in the conns.yaml file
        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))

        dtype = {

            'document_code':                    'String',
            'operation_code':                   'String',
            'state_code_taxpayer':              'String',
            'country_taxpayer':                 'String',
            'country_origin_destiny':           'String',
            'customs_patent':                   'String',
            'number_of_petition':               'String',
            
            'commercial_measure_code':          'UInt8',
            'code_of_dispatch_customs_section': 'UInt16',
            'sequence_of_tariff_fractions':     'UInt16',
            'code_of_entry_customs_section':    'UInt16',
            'tariff_fraction':                  'UInt32',
            'postal_code_taxpayer_address':     'UInt32',

            'unitary_price':                    'Float64',
            'commercial_value':                 'Float64',

            'date_scale':                       'Date'
        }

        # Definition of each step
        step0 = ReadStep()
        step1 = CleanStep()
        step2 = TransformStep()
        step3 = LoadStep('foreign_trade', db_connector, if_exists='drop', pk=['id'], nullable_list=['state_code_taxpayer', 'postal_code_taxpayer_address'], dtype=dtype)

        # Definition of the pipeline and its steps
        pipeline = AdvancedPipelineExecutor(params)
        pipeline = pipeline.next(step0).next(step1).next(step2).next(step3)
        
        return pipeline.run_pipeline()


def run_coverage(params, **kwargs):
    pipeline = CoveragePipeline()
    pipeline.run(params)


if __name__ == '__main__':
    run_coverage({
        'database-connector': 'clickhouse'
    })

