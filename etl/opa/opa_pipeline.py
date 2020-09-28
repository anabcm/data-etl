
import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, Parameter, PipelineStep
from bamboo_lib.steps import DownloadStep, LoadStep
from static import COLUMNS

class TransformStep(PipelineStep):
    def run_step(self, prev, params):

        df = pd.read_csv(prev)

        df.columns = df.columns.str.lower()

        df = df[['id_ramo', 'descripcion_ramo', 'id_ur', 'descripcion_ur', 'descripcion_tipo_ppi', 'id_entidad_federativa', 
                'latitud', 'longitud', 'localizacion', 'fecha_inicio_cal_ff', 'fecha_fin_cal_ff', 'beneficios_esperados', 
                'aprobado', 'modificado', 'ejercido', 'avance_fisico', 'monto_total_inversion', 'ciclo', 'estatus_operacion']].copy()

        # dim replace
        dim_tipo_ppi = dict(zip(list(df['descripcion_tipo_ppi'].unique()), range(1, len(list(df['descripcion_tipo_ppi'].unique())) + 1)))
        dim_estatus_operacion = dict(zip(list(df['estatus_operacion'].unique()), range(1, len(list(df['estatus_operacion'].unique())) + 1)))
        df['descripcion_tipo_ppi'].replace(dim_tipo_ppi, inplace=True)
        df['estatus_operacion'].replace(dim_estatus_operacion, inplace=True)

        # unused columns
        df.drop(columns=['descripcion_ramo', 'descripcion_ur'], inplace=True)

        # date format
        for col in ['fecha_inicio_cal_ff', 'fecha_fin_cal_ff']:
            split = df[col].apply(lambda x: x.split(' ')[0]).str.split('/', expand=True)
            df[col] = split[2] + split[1] + split[0]

        df.rename(columns=COLUMNS, inplace=True)

        for col in ['id_investment_area', 'id_investment_type', 'fiscal_resources_start_date', 'fiscal_resources_end_date']:
            df[col] = df[col].astype(int)

        return df

class OPAPipeline(EasyPipeline):
    @staticmethod
    def steps(params):
        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))

        dtype = {
            'id_investment_area':           'UInt8',
            'id_responsible_unit':          'String',
            'id_investment_type':           'UInt8',
            'ent_id':                       'String',
            'latitude':                     'Float32',
            'longitude':                    'Float32',
            'localization':                 'String',
            'fiscal_resources_start_date':  'UInt32',
            'fiscal_resources_end_date':    'UInt32',
            'budget_cycle':                 'UInt16',
            'status_operation':             'UInt8',
            'expected_benefits':            'String',
            'amount_approved':              'Float64',
            'modified_amount':              'Float64',
            'amount_exercised':             'Float64',
            'physical_advance':             'Float64',
            'amount_total_inversion':       'UInt64'
        }

        download_step = DownloadStep(
            connector='opa-data',
            connector_path='conns.yaml'
        )

        transform_step = TransformStep()
        load_step = LoadStep('budget_opa', db_connector, 
                    if_exists='drop', pk=['id_investment_area', 'id_responsible_unit', 'id_investment_type'], dtype=dtype,
                    nullable_list=['amount_approved', 'modified_amount', 'amount_exercised', 'physical_advance',
                                   'latitude', 'longitude', 'localization']
        )

        return [download_step, transform_step, load_step]

if __name__ == '__main__':
    pp = OPAPipeline()
    pp.run({})