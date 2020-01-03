import pandas as pd
from bamboo_lib.models import PipelineStep
from bamboo_lib.models import EasyPipeline
from bamboo_lib.connectors.models import Connector
from bamboo_lib.steps import DownloadStep
from bamboo_lib.steps import LoadStep
from bamboo_lib.helpers import grab_connector

class ReadStep(PipelineStep):
    def run_step(self, prev, params):
        url = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vRf6ecVlEDTaBNfp2VSd7Ti-AnAQDyQlMjF7uek-cQHQ49ihWv4zeSXgN8z0gJV72ogir3hYvYTu8iX/pub?output=xlsx'
        sector = pd.read_excel(url, dtype='str')
        return df

class CleanStep(PipelineStep):
    def run_step(self, prev, params):
        df = prev
        sector = sector.loc[:, ['sector_id', 'sector_es', 'sector_en']].drop_duplicates().copy()
        return df

class GDPNamesPipeline(EasyPipeline):
    @staticmethod
    def description():
        return 'ETL script for GDP, México'

    @staticmethod
    def website():
        return 'https://www.inegi.org.mx/app/indicadores/?tm=0&t=10200034#D10200034'

    @staticmethod
    def steps(params):
        # Use of connectors specified in the conns.yaml file
        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))
        dtype = {
            'sector_id':            'String',
            'sector_es':            'String',
            'sector_en':            'String',
        }

        read_step = ReadStep()
        clean_step = CleanStep()

        load_step = LoadStep('inegi_gdp_names', db_connector, if_exists='drop', pk=['sector_id'], dtype=dtype)

        return [read_step, clean_step, load_step]