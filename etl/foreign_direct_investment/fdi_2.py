import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import DownloadStep
from bamboo_lib.steps import LoadStep
from helpers import norm, binarice_value
from shared import get_dimensions


class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        data = prev
        df = pd.read_excel(data, sheet_name='2.4')
        df.columns = [norm(x.strip().lower().replace(' ', '_').replace('-', '_').replace('%', 'perc')) for x in df.columns]
        df = df.loc[~df['entidad_federativa'].str.contains('Total')].copy()

        for col in df.columns:
            if (col == 'monto_c') | ('monto_c_' in col):
                df[col] = df[col].apply(lambda x: binarice_value(x))

        # get end_id dimension
        dim_geo = get_dimensions()[0]

        df['entidad_federativa'].replace(dict(zip(dim_geo['ent_name'], dim_geo['ent_id'])), inplace=True)

        df.columns = ['ent_id', 'year', 'quarter_id',
                    'value_between_companies', 'value_new_investments',
                    'value_re_investments', 'count_between_companies',
                    'count_new_investments', 'count_re_investments',
                    'value_between_companies_c', 'value_new_investments_c',
                    'value_re_investments_c']

        df['quarter_id'] = df['year'].astype(int).astype(str) + df['quarter_id'].astype(int).astype(str)
        df['quarter_id'] = df['quarter_id'].astype(int)
        df.fillna(0, inplace=True)

        return df

class FDI2Pipeline(EasyPipeline):
    @staticmethod
    def steps(params):
        db_connector = Connector.fetch("clickhouse-database", open("../conns.yaml"))

        dtype = {
            'ent_id':                    'UInt8',
            'year':                      'UInt16',
            'quarter_id':                'UInt16',
            'value_between_companies':   'Float32',
            'value_new_investments':     'Float32',
            'value_re_investments':      'Float32',
            'count_between_companies':   'UInt16',
            'count_new_investments':     'UInt16',
            'count_re_investments':      'UInt16',
            'value_between_companies_c': 'UInt8',
            'value_new_investments_c':   'UInt8',
            'value_re_investments_c':    'UInt8'
        }

        download_step = DownloadStep(
            connector="fdi-data",
            connector_path="conns.yaml"
        )

        transform_step = TransformStep()
        load_step = LoadStep(
            'fdi_2', db_connector, if_exists="drop", 
            pk=['ent_id'], dtype=dtype
        )

        return [download_step, transform_step, load_step]

if __name__ == "__main__":
    pp = FDI2Pipeline()
    pp.run({})