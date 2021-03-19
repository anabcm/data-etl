
import pandas as pd
import numpy as np
from bamboo_lib.models import PipelineStep, AdvancedPipelineExecutor
from bamboo_lib.models import Parameter, EasyPipeline
from bamboo_lib.connectors.models import Connector
from bamboo_lib.steps import DownloadStep
from bamboo_lib.steps import LoadStep
from bamboo_lib.helpers import grab_connector

class ReadStep(PipelineStep):
    def run_step(self, prev, params):
        # intercensal census data
        df = pd.read_csv(prev[0], encoding='latin-1', dtype={'ENT': 'str', 'MUN': 'str', 'LOC50K': 'str'})
        # data to replace
        data = {}
        for col in ['pisos', 'techos', 'paredes', 'cobertura', 'financiamiento', 'clavivp', 'totcuart', 'cuadorm', 'ingr_ayugob', 'ingr_perotropais', 'deuda', 'forma_adqui', 'refrigerador', 'lavadora', 'autoprop', 'televisor', 'internet', 'computadora', 'celular']:
            data[col] = pd.read_excel(prev[1], sheet_name=col, encoding='latin-1', dtype='object')
        return df, data, prev[1]

class CleanStep(PipelineStep):
    def run_step(self, prev, params):
        df, data, dim_income = prev
        # preformat
        df.columns = df.columns.str.lower()
        # location level
        df['loc_id'] = (df.ent.astype('str') + df.mun.astype('str') + df.loc50k.astype('str')).astype('int')
        # column type conversion
        df['ingtrhog'] = df['ingtrhog'].astype('float')
        df.ingtrhog = df.ingtrhog.fillna(-5).round(0).astype('int64')
        # nan management
        for col in ['refrigerador', 'lavadora', 'autoprop', 'televisor', 'internet', 'computadora', 'celular']:
            df[col].fillna(0, inplace=True)
        return df, data, dim_income

class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        df, data, dim_income = prev
        # replace, select and group data
        for col in data.keys():
            df[col] = df[col].replace(dict(zip(data[col]['prev_id'], data[col]['id'].astype('int'))))

        # income interval replace
        income = pd.read_excel(dim_income, sheet_name='income', encoding='latin-1')
        for ing in df.ingtrhog.unique():
            for level in range(income.shape[0]):
                if (ing >= income.interval_lower[level]) & (ing < income.interval_upper[level]):
                    df.ingtrhog = df.ingtrhog.replace(ing, str(income.id[level]))
                    break
                if ing >= income.interval_upper[income.shape[0]-1]:
                    df.ingtrhog = df.ingtrhog.replace(ing, str(income.id[income.shape[0]-1]))
                    break
        df.ingtrhog = df.ingtrhog.astype('int')
        df.ingtrhog.replace(-5, np.nan, inplace=True)

        labels = ['loc_id', 'cobertura', 'ingtrhog', 'pisos', 'techos', 'paredes', 'forma_adqui', 
                'deuda', 'numpers', 'financiamiento', 'totcuart', 'cuadorm', 'clavivp', 
                'ingr_ayugob', 'ingr_perotropais', 'refrigerador', 'lavadora', 'autoprop', 
                'televisor', 'internet', 'computadora', 'celular']

        # subset of columns
        df = df[['factor'] + labels].copy()
        df.fillna('temp', inplace=True)
        df = df.groupby(labels).sum().reset_index(col_fill='ffill')
        df = df.rename(columns={'factor': 'households', 'pisos': 'floor', 'paredes': 'wall', 'techos': 'roof', 'forma_adqui': 'acquisition', 'deuda': 'debt', 'ingtrhog': 'income', 'cobertura': 'coverage', 
                                'clavivp': 'home_type', 'financiamiento': 'funding', 'ingr_ayugob': 'government_financial_aid', 'ingr_perotropais': 'foreign_financial_aid', 'numpers': 'n_inhabitants', 
                                'totcuart': 'total_rooms', 'cuadorm': 'bedrooms', 'refrigerador': 'fridge', 'lavadora': 'washing_machine', 'autoprop': 'vehicle', 'televisor': 'tv', 'computadora': 'computer', 'celular': 'mobile_phone'})
        df.replace('temp', np.nan, inplace=True)

        # data types
        for col in df.columns:
            df[col] = df[col].astype('float')

        df['year'] = 2015
        df['water_pump'] = np.nan
        df['solar_heater'] = np.nan
        df['air_conditioner'] = np.nan
        df['solar_panel'] = np.nan
        df['organic_trash'] = np.nan
        df['oven'] = np.nan
        df['motorcycle'] = np.nan
        df['bicycle'] = np.nan
        df['tv_service'] = np.nan
        df['movie_service'] = np.nan
        df['video_game_console'] = np.nan
        df['title_deed'] = np.nan
        df['debt'] = np.nan
        df['sex'] = np.nan
        df['age'] = np.nan
        df['national_financial_aide'] = np.nan
        df['retirement_financial_aid'] = np.nan

        return df

class HousingPipeline(EasyPipeline):
    @staticmethod
    def description():
        return 'ETL script for Intercensal Housing Census 2015, México'

    @staticmethod
    def website():
        return 'https://www.inegi.org.mx/programas/intercensal/2015/'

    @staticmethod
    def parameter_list():
        return [
            Parameter(label='Index', name='index', dtype=str),
            Parameter(label='Source connector', name='source-connector', dtype=str, source=Connector)
        ]

    @staticmethod
    def steps(params):
        # Use of connectors specified in the conns.yaml file
        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))
        dtype = {
            'loc_id':                   'UInt32',
            'households':               'UInt16',
            'floor':                    'UInt8',
            'wall':                     'UInt8',
            'roof':                     'UInt8',
            'acquisition':              'UInt8',
            'debt':                     'UInt8',
            'income':                   'UInt8',
            'coverage':                 'UInt8',
            'home_type':                'UInt8',
            'funding':                  'UInt8',
            'government_financial_aid': 'UInt8',
            'foreign_financial_aid':    'UInt8',
            'n_inhabitants':            'UInt8',
            'total_rooms':              'UInt8',
            'bedrooms':                 'UInt8',
            'fridge':                   'UInt8',
            'washing_machine':          'UInt8',
            'vehicle':                  'UInt8',
            'tv':                       'UInt8',
            'computer':                 'UInt8',
            'mobile_phone':             'UInt8',
            'internet':                 'UInt8',
            'year':                     'UInt16',
            'water_pump':               'UInt8',
            'solar_heater':             'UInt8',
            'air_conditioner':          'UInt8',
            'solar_panel':              'UInt8',
            'organic_trash':            'UInt8',
            'oven':                     'UInt8',
            'motorcycle':               'UInt8',
            'bicycle':                  'UInt8',
            'tv_service':               'UInt8',
            'movie_service':            'UInt8',
            'video_game_console':       'UInt8',
            'title_deed':               'UInt8',
            'sex':                      'UInt8',
            'age':                      'UInt8',
            'national_financial_aid':   'UInt8',
            'retirement_financial_aid': 'UInt8'
        }

        download_step = DownloadStep(
            connector=['housing-data', 'labels']
            connector_path='conns.yaml'
        )

        read_step = ReadStep()
        clean_step = CleanStep()
        transform_step = TransformStep()

        load_step = LoadStep(
            'inegi_housing', db_connector, if_exists='append', pk=['loc_id'], dtype=dtype, 
            nullable_list=['households', 'floor', 'wall', 'roof', 'acquisition', 'debt', 'income', 'coverage',
                          'home_type', 'funding', 'government_financial_aid', 'foreign_financial_aid',
                          'n_inhabitants', 'total_rooms', 'bedrooms', 'fridge', 'washing_machine', 
                          'vehicle', 'tv', 'computer', 'mobile_phone', 'internet', 'water_pump', 'solar_heater', 'air_conditioner', 
                          'solar_panel', 'organic_trash', 'oven', 'motorcycle', 'bicycle', 'tv_service', 'movie_service', 
                          'video_game_console', 'title_deed', 'sex', 'age', 'national_financial_aid', 'retirement_financial_aid']
        )

        return [download_step, read_step, clean_step, transform_step, load_step]