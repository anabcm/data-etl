import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import DownloadStep
from bamboo_lib.steps import LoadStep


class ExtractStep(PipelineStep):
    def run_step(self, prev, params):
        names = [
            'indicator_id', 'topic', 'indicator_name', 'short_definition',
            'long_definition', 'unit_of_measure', 'periodicity', 'base_period',
            'other_notes', 'aggregation_method', 'limitations_and_expectations',
            'notes_from_original_source', 'general_comments', 'source',
            'statistical_concept_and_methodology', 'development_relevance',
            'related_source_links', 'other_web_links', 'related_indicators',
            'license_type', 'empty_column'
        ]

        df = pd.read_csv(prev, header=0, names=names)
        df.drop('empty_column', axis=1, inplace=True)

        return df


class DimIndicatorsPipeline(EasyPipeline):
    @staticmethod
    def steps(params):
        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))

        dtype = {
            'indicator_id':                        'String',
            'topic':                               'String',
            'indicator_name':                      'String',
            'short_definition':                    'String',
            'long_definition':                     'String',
            'unit_of_measure':                     'String',
            'periodicity':                         'String',
            'base_period':                         'String',
            'other_notes':                         'String',
            'aggregation_method':                  'String',
            'limitations_and_expectations':        'String',
            'notes_from_original_source':          'String',
            'general_comments':                    'String',
            'source':                              'String',
            'statistical_concept_and_methodology': 'String',
            'development_relevance':               'String',
            'related_source_links':                'String',
            'other_web_links':                     'String',
            'related_indicators':                  'String',
            'license_type':                        'String'
        }

        nullable_list = list(dtype.keys())[2:]

        download_step = DownloadStep(
            connector='wdi-series',
            connector_path="conns.yaml"
        )
        extract_step = ExtractStep()
        load_step = LoadStep(
            "dim_shared_indicators", db_connector, if_exists="append", dtype=dtype,
            pk=['indicator_id'],
            nullable_list=nullable_list
        )

        return [download_step, extract_step, load_step]


if __name__ == '__main__':
    pipeline = DimIndicatorsPipeline()
    pipeline.run({})