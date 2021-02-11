
import numpy as np
import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, PipelineStep, Parameter
from bamboo_lib.steps import DownloadStep, LoadStep
from static import FILL_COLUMNS, CENSUS_2019_RENAME


def fill_level(df, columns):
    for col in columns:
        if col not in df.columns:
            df[col] = 0
    return df


class ReadStep(PipelineStep):
    def run_step(self, prev, params):

        df = pd.read_excel(prev, header=11)

        df.columns = CENSUS_2019_RENAME

        FILL_COLUMNS = ['ent_id', 'mun_id', 'sector_id', 'subsector_id', 'rama_id']

        base = df.loc[df['ent_id'] != '00 NAL'].copy()
        df = pd.DataFrame()

        return base

class EntSec(PipelineStep):
    def run_step(self, prev, params):
        base = prev

        df = base.copy()

        df = df.loc[df['rama_id'].isna()].copy()
        df = df.loc[df['subsector_id'].isna()].copy()
        df = df.loc[~df['sector_id'].isna()].copy()
        df = df.loc[df['mun_id'].isna()].copy()

        df.drop(columns=['mun_id', 'subsector_id', 'rama_id', 'denominacion'], inplace=True)

        df['ent_id'] = df['ent_id'].str.split(' ', expand=True)[0].str.strip().astype(int)

        df['sector_id'] = df['sector_id'].str.split(' ', expand=True)[1].str.strip()

        df = fill_level(df, FILL_COLUMNS)

        # Entidad-Sector
        df['level'] = 1

        df_ent_sec = df.copy()

        return df_ent_sec, base

class EntSubsector(PipelineStep):
    def run_step(self, prev, params):
        df_ent_sec, base = prev

        df = base.copy()

        df = df.loc[df['rama_id'].isna()].copy()
        df = df.loc[~df['subsector_id'].isna()].copy()
        df = df.loc[df['mun_id'].isna()].copy()

        df.drop(columns=['mun_id', 'sector_id', 'rama_id', 'denominacion'], inplace=True)

        df['ent_id'] = df['ent_id'].str.split(' ', expand=True)[0].str.strip().astype(int)

        df['subsector_id'] = df['subsector_id'].str.split(' ', expand=True)[1].str.strip()

        df = fill_level(df, FILL_COLUMNS)

        # Entidad-Subsector
        df['level'] = 2

        df_ent_sub = df.copy()

        return df_ent_sec, df_ent_sub, base

class EntRama(PipelineStep):
    def run_step(self, prev, params):
        df_ent_sec, df_ent_sub, base = prev

        df = base.copy()

        df = df.loc[~df['rama_id'].isna()].copy()
        df = df.loc[df['mun_id'].isna()].copy()

        df.drop(columns=['mun_id', 'sector_id', 'subsector_id', 'denominacion'], inplace=True)

        df['ent_id'] = df['ent_id'].str.split(' ', expand=True)[0].str.strip().astype(int)

        df['rama_id'] = df['rama_id'].str.split(' ', expand=True)[1].str.strip()

        df = fill_level(df, FILL_COLUMNS)

        # Entidad-Rama
        df['level'] = 3

        df_ent_ram = df.copy()

        return df_ent_sec, df_ent_sub, df_ent_ram, base

class MunSec(PipelineStep):
    def run_step(self, prev, params):
        df_ent_sec, df_ent_sub, df_ent_ram, base = prev

        df = base.copy()

        df = df.loc[df['rama_id'].isna()].copy()
        df = df.loc[df['subsector_id'].isna()].copy()
        df = df.loc[~df['sector_id'].isna()].copy()
        df = df.loc[~df['mun_id'].isna()].copy()

        df['mun_id'] = df['mun_id'].str.split(' ', expand=True)[0].str.strip().astype(int)

        df.drop(columns=['ent_id', 'subsector_id', 'rama_id', 'denominacion'], inplace=True)

        df['sector_id'] = df['sector_id'].str.split(' ', expand=True)[1].str.strip()

        df = fill_level(df, FILL_COLUMNS)

        # Municipio-Sector
        df['level'] = 4

        df_mun_sec = df.copy()

        return df_ent_sec, df_ent_sub, df_ent_ram, df_mun_sec

class JoinStep(PipelineStep):
    def run_step(self, prev, params):
        df_ent_sec, df_ent_sub, df_ent_ram, df_mun_sec = prev

        df = pd.DataFrame()
        for _df in [df_ent_sec, df_ent_sub, df_ent_ram, df_mun_sec]:
            df = df.append(_df, sort=False)

        df[list(df.columns[df.columns != 'sector_id'])] = df[list(df.columns[df.columns != 'sector_id'])].astype(float)
        df['sector_id'] = df['sector_id'].astype(str)
        df['level'] = df['level'].astype(int)

        df['year'] = 2019

        return df

class EconomicCensusPipeline(EasyPipeline):
    @staticmethod
    def steps(params, **kwargs):
        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))

        dtypes = {
            'ent_id':                                                       'UInt8',
            'mun_id':                                                       'UInt16',
            'sector_id':                                                    'String', 
            'subsector_id':                                                 'UInt16',
            'rama_id':                                                      'UInt16',
            'UE':                                                           'UInt32',
            'total_personal_ocupado':                                       'Float64',
            'total_personal_ocupado_dependiente':                           'Float64',
            'personal_ocupado_remunerado':                                  'Float64',
            'propietarios_familiares_otros_no_remunerados':                 'Float64',
            'no_dependiente_razon_social':                                  'Float64',
            'remuneraciones':                                               'Float64',
            'gasto_consumo_bienes_servicios':                               'Float64',
            'ingreso_suministro_bienes_servicios':                          'Float64',
            'produccion_total_bruta':                                       'Float64',
            'consumo_intermedio':                                           'Float64',
            'valor_agregado_censal_bruto':                                  'Float64',
            'formacion_bruta_capital_fijo':                                 'Float64',
            'variacion_total_existencias':                                  'Float64',
            'activos_fijos':                                                'Float64',
            'depreciacion_activos_fijos':                                   'Float64',
            'participacion_remuneraciones_gastos_consumo_bienes_servicios': 'Float64',
            'valor_agregado_censal_bruto_produccion_bruta_total':           'Float64',
            'tasa_rentabilidad_promedio':                                   'Float64',
            'activos_fijos_produccion_bruta_total':                         'Float64',
            'participacion_depreciacion_valor_Activos_fijos':               'Float64',
            'margen_bruto_operacion':                                       'Float64',
            'participacion_consumo_intermedio_produccion_bruta_total':      'Float64',
            'valor_agregado_total_activos_fijos':                           'Float64',
            'valor_agregado_promedio_persona_ocupada':                      'Float64',
            'produccion_bruta_total_personal_ocupado_total':                'Float64',
            'remuneracion_media_persona_remunerada':                        'Float64',
            'valor_activos_fijos_persona_ocupada':                          'Float64',
            'year':                                                         'UInt16',
            'level':                                                        'UInt8'
        }

        download_step = DownloadStep(
            connector='economic-census-main-features',
            connector_path='conns.yaml'
        )

        read_step = ReadStep()
        ent_sector = EntSec()
        ent_subsector = EntSubsector()
        ent_rama = EntRama()
        mun_Sec = MunSec()
        join_step = JoinStep()

        load_step = LoadStep(
            'inegi_economic_census_main_features', db_connector, dtype=dtypes, if_exists='drop', 
            pk=['mun_id', 'ent_id', 'year'], nullable_list = ['total_personal_ocupado', 'total_personal_ocupado_dependiente', 
                'personal_ocupado_remunerado', 'propietarios_familiares_otros_no_remunerados', 'no_dependiente_razon_social',
                'remuneraciones', 'gasto_consumo_bienes_servicios', 'ingreso_suministro_bienes_servicios', 'produccion_total_bruta', 
                'consumo_intermedio', 'valor_agregado_censal_bruto', 'formacion_bruta_capital_fijo', 'variacion_total_existencias', 
                'activos_fijos', 'depreciacion_activos_fijos', 'participacion_remuneraciones_gastos_consumo_bienes_servicios',
                'valor_agregado_censal_bruto_produccion_bruta_total', 'tasa_rentabilidad_promedio', 'activos_fijos_produccion_bruta_total', 
                'participacion_depreciacion_valor_Activos_fijos', 'margen_bruto_operacion', 'participacion_consumo_intermedio_produccion_bruta_total', 
                'valor_agregado_total_activos_fijos', 'valor_agregado_promedio_persona_ocupada', 'produccion_bruta_total_personal_ocupado_total',
                'remuneracion_media_persona_remunerada', 'valor_activos_fijos_persona_ocupada']
        )

        return [download_step, read_step, ent_sector, ent_subsector, ent_rama, mun_Sec, join_step, load_step]

if __name__ == '__main__':
    pp = EconomicCensusPipeline()
    pp.run({})
