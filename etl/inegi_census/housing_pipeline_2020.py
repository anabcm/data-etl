import pandas as pd
import numpy as np
from bamboo_lib.models import Parameter, EasyPipeline, PipelineStep
from bamboo_lib.connectors.models import Connector
from bamboo_lib.steps import LoadStep, DownloadStep

class ReadStep(PipelineStep):
    def run_step(self, prev, params):
        df = pd.read_csv(prev[0])
        df.columns = df.columns.str.lower()
        return df, prev[1]

class CleanStep(PipelineStep):
    def run_step(self, prev, params):
        df, dimension = prev

        df.rename(columns={'financiamiento1': 'financiamiento'}, inplace=True)
        
        labels = ['clavivp', 'forma_adqui', 'paredes', 'techos', 'pisos', 'cuadorm', 
                'totcuart', 'numpers', 'ingtrhog', 'refrigerador', 'lavadora', 
                'autoprop', 'televisor', 'internet', 'computadora', 'celular', 'cobertura', 
                'financiamiento', 'ingr_ayugob', 'ingr_perotropais']

        extra_labels = ['bomba_agua', 'calentador_solar', 'aire_acon', 'panel_solar', 
        'separacion1', 'horno', 'motocicleta', 'bicicleta', 'serv_tv_paga', 'serv_pel_paga', 
        'con_vjuegos', 'escrituras', 'deuda', 'jefe_sexo', 'jefe_edad', 'ingr_perdentpais',
        'ingr_jubpen']      

        df = df[['ent', 'mun', 'loc50k', 'factor'] + labels + extra_labels].copy()

        dtypes = {
            'ent': 'str',
            'mun': 'str',
            'loc50k': 'str',
            'clavivp': 'int',
            'forma_adqui': 'int',
            'paredes': 'int',
            'techos': 'int',
            'pisos': 'int',
            'cuadorm': 'int',
            'totcuart': 'int',
            'numpers': 'int',
            'ingtrhog': 'float',
            'factor': 'int',
            'refrigerador': 'int',
            'lavadora': 'int', 
            'autoprop': 'int', 
            'televisor': 'int', 
            'internet': 'int', 
            'computadora': 'int', 
            'celular': 'int',
            'bomba_agua': 'int',
            'calentador_solar': 'int',
            'aire_acon': 'int',
            'panel_solar': 'int',
            'separacion1': 'int',
            'horno': 'int',
            'motocicleta': 'int',
            'bicicleta': 'int',
            'serv_tv_paga': 'int',
            'serv_pel_paga': 'int',
            'con_vjuegos': 'int',
            'escrituras': 'int',
            'deuda': 'int',
            'jefe_sexo': 'int',
            'jefe_edad': 'int',
            'cobertura': 'int', 
            'financiamiento': 'int', 
            'ingr_ayugob': 'int', 
            'ingr_perotropais': 'int',
            'ingr_perdentpais': 'int',
            'ingr_jubpen': 'int'
        }

        for key, val in dtypes.items():
            try:
                df.loc[:, key] = df[key].astype(val)
                continue
            except:
                df.loc[:, key] = df[key].astype('float')

        return df, labels, extra_labels, dimension

class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        df, labels, extra_labels, dimension = prev

        # data to replace
        data = {}
        for col in ['clavivp', 'paredes', 'techos', 'pisos', 'cuadorm', 'totcuart', 'refrigerador', 'lavadora', 'autoprop', 'televisor', 'internet', 'computadora', 'celular', 'cobertura', 'financiamiento_2020', 'ingr_ayugob', 'ingr_perotropais']:
            data[col] = pd.read_excel(dimension, sheet_name=col, dtype='object')
        data['financiamiento'] = data.pop('financiamiento_2020')

        # location id
        df['loc_id'] = (df.ent.astype('str').str.zfill(2) + df.mun.astype('str').str.zfill(3) + df.loc50k.astype('str').str.zfill(4)).astype('int')
        df.drop(columns=['ent', 'mun', 'loc50k'], inplace=True)
        df.ingtrhog = df.ingtrhog.fillna(-5).round(0).astype('int64')

        # income interval replace
        income = pd.read_excel(dimension, sheet_name='income')
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

        # ids replace
        for col in data.keys():
            df[col] = df[col].replace(dict(zip(data[col]['prev_id'], data[col]['id'].astype('int'))))
        
        # groupby data
        df.fillna('temp', inplace=True)
        df = df.groupby(['loc_id'] + labels + extra_labels).sum().reset_index(col_fill='ffill')
        df = df.rename(columns={'factor': 'households', 
                                'clavivp': 'home_type',
                                'forma_adqui': 'acquisition',
                                'paredes': 'wall', 
                                'techos': 'roof',
                                'pisos': 'floor',
                                'cuadorm': 'bedrooms',
                                'totcuart': 'total_rooms',
                                'numpers': 'n_inhabitants',
                                'ingtrhog': 'income', 
                                'refrigerador': 'fridge', 
                                'lavadora': 'washing_machine', 
                                'autoprop': 'vehicle', 
                                'televisor': 'tv', 
                                'computadora': 'computer', 
                                'celular': 'mobile_phone',
                                'bomba_agua': 'water_pump',
                                'calentador_solar': 'solar_heater',
                                'aire_acon': 'air_conditioner',
                                'panel_solar': 'solar_panel',
                                'separacion1': 'organic_trash',
                                'horno': 'oven',
                                'motocicleta': 'motorcycle',
                                'bicicleta': 'bicycle',
                                'serv_tv_paga': 'tv_service',
                                'serv_pel_paga': 'movie_service',
                                'con_vjuegos': 'video_game_console',
                                'escrituras': 'title_deed',
                                'deuda': 'debt',
                                'jefe_sexo': 'sex',
                                'jefe_edad': 'age',
                                'cobertura': 'coverage',
                                'financiamiento': 'funding',
                                'ingr_ayugob': 'government_financial_aid', 
                                'ingr_perotropais': 'foreign_financial_aid',
                                'ingr_perdentpais': 'national_financial_aid', 
                                'ingr_jubpen': 'retirement_financial_aid'})

        df.replace('temp', np.nan, inplace=True)
        
        # data type conversion
        for col in df.columns:
            df[col] = df[col].astype('float')
        
        df['year'] = 2020

        return df

class HousingPipeline(EasyPipeline):
    @staticmethod
    def description():
        return 'ETL script for Intercensal Housing Census 2020, México'

    @staticmethod
    def website():
        return 'http://datawheel.us'

    @staticmethod
    def parameter_list():
        return [
            Parameter(label="Index", name="index", dtype=str)
        ]

    @staticmethod
    def steps(params, **kwargs):
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
            connector=["housing-data-2020", "labels"],
            connector_path="conns.yaml",
            force=True
        )

        read_step = ReadStep()
        clean_step = CleanStep()
        transform_step = TransformStep()
        load_step = LoadStep(
            'inegi_housing', db_connector, if_exists='append', pk=['loc_id'], dtype=dtype, 
            nullable_list=['income', 'computer', 'coverage', 'n_inhabitants', 'foreign_financial_aid', 'tv', 
                           'wall', 'government_financial_aid', 'video_game_console', 'households', 
                           'mobile_phone', 'debt', 'washing_machine', 'bedrooms', 'air_conditioner', 
                           'movie_service', 'tv_service', 'solar_heater', 'retirement_financial_aid', 
                           'funding', 'organic_trash', 'sex', 'motorcycle', 'fridge', 'oven', 'title_deed', 
                           'solar_panel', 'national_financial_aid', 'floor', 'home_type', 'water_pump', 
                           'total_rooms', 'age', 'bicycle', 'internet', 'vehicle', 'acquisition', 'roof']
        )
        
        return [download_step, read_step, clean_step, transform_step, load_step]

if __name__ == "__main__":
    pp = HousingPipeline()
    for index in range(1, 1 + 32):
        pp.run({
            "index": str(index).zfill(2)
            })