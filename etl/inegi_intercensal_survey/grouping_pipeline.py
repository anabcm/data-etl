import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import DownloadStep
from bamboo_lib.steps import LoadStep


class ExtractStep(PipelineStep):
    def run_step(self, prev, params):
        df_labels = pd.ExcelFile(prev)
        df = pd.read_excel(df_labels, 'totcuart', dtype={
            "id": "int",
            "name_es": "str",
            "name_en": "str",
            "group_id": "int",
            "group_name_es": "str",
            "group_name_en": "str",
        })
        df = df.drop(columns=['prev_id'])
        return df


class DimHousingGroupPipeline(EasyPipeline):
    @staticmethod
    def steps(params):
        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))

        dtype = {
            'id':             'UInt8',
            'name_es':        'String',
            'name_en':        'String',
            'group_id':       'UInt8',
            'group_name_es':  'String',
            'group_name_en':  'String'
        }
        download_step = DownloadStep(
            connector='labels',
            connector_path='conns.yaml'
        )
        extract_step = ExtractStep()
        load_step = LoadStep(
            'dim_housing_group', db_connector, if_exists='drop', dtype=dtype,
            pk=['id']
        )

        return [download_step, extract_step, load_step]

if __name__ == '__main__':
    pp = DimHousingGroupPipeline()
    pp.run({})