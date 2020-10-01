
import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, Parameter, PipelineStep
from bamboo_lib.steps import DownloadStep, LoadStep
from static import COLUMNS


class TransformStep(PipelineStep):
    def run_step(self, prev, params):

        df = pd.read_csv(prev, encoding='latin-1')

        df.columns = df.columns.str.lower()

        df = df[['folio', 'ciclo', 'trimestre', 'id_categoria', 'categoria',
                 'monto_global_aprobado', 'id_entidad_federativa', 'id_municipio_responsable',
                 'id_tipo_programa_proyecto', 'tipo_programa_proyecto', 
                 'id_clasificacion', 'clasificacion',
                 'fecha_inicio', 'fecha_estimada_termino', 
                 'monto_recaudado', 'monto_comprometido', 
                 'monto_devengado', 'monto_ejercido', 'monto_pagado',
                 'id_estatus', 'estatus', 'flujo']].copy()
        
        # mun_id
        df['id_entidad_federativa'] = df['id_entidad_federativa'].astype(int).astype(str).str.zfill(2)
        df['id_municipio_responsable'] = df['id_municipio_responsable'].astype(int).astype(str).str.zfill(3)
        df['mun_id'] = (df['id_entidad_federativa'] + df['id_municipio_responsable']).astype(int)

        df['report_period'] = (df['ciclo'].astype(str) + df['trimestre'].astype(str)).astype(int)

        df['count'] = 1

        # unused columns
        df.drop(columns=['folio', 'categoria', 'id_entidad_federativa', 'flujo',
                 'id_municipio_responsable', 'tipo_programa_proyecto', 
                 'estatus', 'clasificacion', 'ciclo', 'trimestre'], inplace=True)

        # date format
        """for col in ['fecha_inicio', 'fecha_estimada_termino']:
            split = df[col].apply(lambda x: x.split(' ')[0]).str.split('/', expand=True)
            df[col] = split[2] + split[1] + split[0]"""

        # rename columns
        df.rename(columns=COLUMNS, inplace=True)
        df.rename(columns={
            'id_estatus': 'status_id',
            'id_tipo_programa_proyecto': 'project_type_id',
            'id_categoria': 'category_id',
            'id_clasificacion': 'classification_id',
            'fecha_inicio': 'start_date_id',
            'fecha_estimada_termino': 'end_date_id',
            'monto_global_aprobado': 'global_approved_value',
            'monto_recaudado': 'collected_amount',
            'monto_comprometido': 'committed_amount',
            'monto_devengado': 'accrued_amount',
            'monto_ejercido': 'exercised_amount',
            'monto_pagado': 'paid_amount'
        }, inplace=True)

        # non specified project type
        df['project_type_id'].fillna(9, inplace=True)

        for col in ['global_approved_value', 'collected_amount', 
                    'committed_amount', 'accrued_amount',
                    'exercised_amount', 'paid_amount']:
            df[col] = df[col].astype(float)

        return df

class OPAPipeline(EasyPipeline):
    @staticmethod
    def steps(params):
        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))

        dtype = {
            'category_id':           'UInt8',
            'project_type_id':       'UInt8',
            'classification_id':     'UInt8',
            'status_id':             'UInt8',
            'mun_id':                'UInt16',
            'report_period':         'UInt16',
            'count':                 'UInt32',
            'start_date_id':         'String',
            'end_date_id':           'String',
            'global_approved_value': 'Float64',
            'collected_amount':      'Float64',
            'committed_amount':      'Float64',
            'accrued_amount':        'Float64',
            'exercised_amount':      'Float64',
            'paid_amount':           'Float64'
        }

        download_step = DownloadStep(
            connector='opa-tracking-data',
            connector_path='conns.yaml'
        )

        transform_step = TransformStep()
        load_step = LoadStep('opa_resource_tracking', db_connector, 
                    if_exists='drop', pk=['mun_id'], dtype=dtype,
                    nullable_list=['start_date_id', 'end_date_id', 
                                   'collected_amount', 'committed_amount', 'accrued_amount',
                                   'exercised_amount', 'paid_amount']
        )

        return [download_step, transform_step, load_step]

if __name__ == '__main__':
    pp = OPAPipeline()
    pp.run({})