import numpy as np
import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import DownloadStep
from bamboo_lib.steps import LoadStep
from helpers import norm
from shared import get_dimensions, INVESTMENT_TYPE


class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        data = prev
        df = pd.read_excel(data, sheet_name='2.4')
        df.columns = [norm(x.strip().lower().replace(' ', '_').replace('-', '_').replace('%', 'perc')) for x in df.columns]
        df = df.loc[~df['entidad_federativa'].str.contains('Total')].copy()

        for col in df.columns:
            if (col == 'monto_c') | ('monto_c_' in col):
                df.loc[df[col].astype(str).str.lower() == 'c', col] = np.nan
                df[col] = df[col].astype(float)

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

        df.drop(columns=['value_between_companies', 'value_new_investments', 'value_re_investments'], inplace=True)

        base = ['ent_id', 'year', 'quarter_id']
        df_final = pd.DataFrame()
        for option in ['between_companies', 'new_investments', 're_investments']:
            temp = df[base + ['count_{}'.format(option), 'value_{}_c'.format(option)]]
            temp.columns = ['ent_id', 'year', 'quarter_id', 'count', 'value_c']
            temp.dropna(subset=['value_c'], inplace=True)
            temp['investment_type'] = option
            df_final = df_final.append(temp)
        df = df_final.copy()

        df['investment_type'].replace(INVESTMENT_TYPE, inplace=True)

        return df

class FDI2Pipeline(EasyPipeline):
    @staticmethod
    def steps(params):
        db_connector = Connector.fetch("clickhouse-database", open("../conns.yaml"))

        dtype = {
            'ent_id':           'UInt8',
            'year':             'UInt16',
            'quarter_id':       'UInt16',
            'investment_type':  'UInt8',
            'count':            'UInt16',
            'value_c':          'Float32'
        }

        download_step = DownloadStep(
            connector="fdi-data",
            connector_path="conns.yaml"
        )

        transform_step = TransformStep()
        load_step = LoadStep(
            'fdi_2', db_connector, if_exists="drop", 
            pk=['ent_id'], dtype=dtype, 
            nullable_list=['count']
        )

        return [download_step, transform_step, load_step]

if __name__ == "__main__":
    pp = FDI2Pipeline()
    pp.run({})