
import pandas as pd
from bamboo_lib.models import PipelineStep
from bamboo_lib.models import EasyPipeline
from bamboo_lib.connectors.models import Connector
from bamboo_lib.steps import DownloadStep
from bamboo_lib.steps import LoadStep
from bamboo_lib.helpers import grab_connector

class ReadStep(PipelineStep):
    def run_step(self, prev, params):
        df = pd.read_excel('https://storage.googleapis.com/datamexico-data/gdp/Indicadores20191226081031.xls', header=4)
        return df

class CleanStep(PipelineStep):
    def run_step(self, prev, params):

        df = prev

        levels = {'farming': 'Total sector 11',
                'mining': 'Total sector 21',
                'energy_transmission': 'Total sector 22',
                'construction': 'Total sector 23',
                'manufacturing': 'Total sector 31-33',
                'wholesale_trade': '43 Comercio al por mayor',
                'retail_trade': '46 Comercio al por menor',
                'transportation_warehousing': 'Total sector 48-49',
                'information': 'Total sector 51',
                'finance_insurance': 'Total sector 52',
                'real_state': 'Total sector 53',
                'professional_services': '54 Servicios profesionales, científicos y técnicos',
                'management_companies': '55 Corporativos',
                'administrative_support_services': '56 Servicios de apoyo a los negocios y manejo de desechos y servicios de remediación',
                'educational_services': '61 Servicios educativos',
                'health_care': 'Total sector 62',
                'arts': '71 Servicios de esparcimiento culturales y deportivos, y otros servicios recreativos',
                'accomodation_food_services':'Total sector 72',
                'other_services_no_government': 'Total sector 81',
                'government_activities': ' 93 Actividades legislativas, gubernamentales, de impartición de justicia y de organismos internacionales y extraterritoriales'}

        mask = []
        for k, v in levels.items():
            for col in df.columns:
                if v in col:
                    mask.append(col)
                    break

        df = df.loc[:, ['Periodos'] + mask].copy()

        df.columns = ['quarter_id'] + [col for col in levels.keys()]

        df.dropna(inplace=True)

        df.quarter_id = df.quarter_id.str.replace('/p1', '').str.replace('/r1', '').str.replace('/0', '').str.strip()

        for col in df.columns:
            df[col] = df[col].astype('float')

        return df

class GDPPipeline(EasyPipeline):
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
            'quarter_id':                        'UInt32',
            'farming':                           'UInt32',
            'mining':                            'UInt32',
            'energy_transmission':               'UInt32',
            'construction':                      'UInt32',
            'manufacturing':                     'UInt32',
            'wholesale_trade':                   'UInt32',
            'retail_trade':                      'UInt32',
            'transportation_warehousing':        'UInt32',
            'information':                       'UInt32',
            'finance_insurance':                 'UInt32',
            'real_state':                        'UInt32',
            'professional_services':             'UInt32',
            'management_companies':              'UInt32',
            'administrative_support_services':   'UInt32',
            'educational_services':              'UInt32',
            'health_care':                       'UInt32',
            'arts':                              'UInt32',
            'accomodation_food_services':        'UInt32',
            'other_services_no_government':      'UInt32',
            'government_activities':             'UInt32'
        }

        read_step = ReadStep()
        clean_step = CleanStep()

        load_step = LoadStep('inegi_gdp', db_connector, if_exists='drop', pk=['quarter_id'], dtype=dtype)

        return [read_step, clean_step, load_step]