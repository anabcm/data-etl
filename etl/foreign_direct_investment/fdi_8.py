import numpy as np
import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import DownloadStep
from bamboo_lib.steps import LoadStep
from helpers import norm
from shared import SECTOR_REPLACE, INVESTMENT_TYPE


class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        data = prev
        df = pd.read_excel(data, sheet_name=params.get('sheet_name'))
        df.columns = [norm(x.strip().lower().replace(' ', '_').replace('-', '_').replace('%', 'perc')) for x in df.columns]
        df = df.loc[~df[params.get('level')].str.contains('Total')].copy()

        for col in df.columns:
            if (col == 'monto_c') | ('monto_c_' in col):
                df.loc[df[col].astype(str).str.lower() == 'c', col] = np.nan
                df[col] = df[col].astype(float)

        split = df[params.get('level')].str.split(' ', n=1, expand=True)
        df[params.get('level')] = split[0]

        df.columns = params.get('columns')

        if params.get('pk') == 'sector_id':
            df['sector_id'].replace(SECTOR_REPLACE, inplace=True)

        else:
            df[params.get('pk')] = df[params.get('pk')].astype(int)

        df['quarter_id'] = df['year'].astype(int).astype(str) + df['quarter_id'].astype(int).astype(str)
        df['quarter_id'] = df['quarter_id'].astype(int)

        df.drop(columns=['year', 'value_between_companies', 
                         'value_new_investments', 'value_re_investments'], inplace=True)
        
        base = list(df.columns[:2])
        df_final = pd.DataFrame()
        for option in ['between_companies', 'new_investments', 're_investments']:
            temp = df[base + ['count_{}'.format(option), 'value_{}_c'.format(option)]]
            temp.columns = base + ['count', 'value_c']
            temp.dropna(subset=['value_c'], inplace=True)
            temp['investment_type'] = option
            df_final = df_final.append(temp)
        df = df_final.copy()

        df['investment_type'].replace(INVESTMENT_TYPE, inplace=True)

        return df

class FDI8Pipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return [
            Parameter(name='pk', dtype=str),
            Parameter(name='sheet_name', dtype=str),
            Parameter(name='level', dtype=str),
            Parameter(name='dtype', dtype=str),
            Parameter(name='columns', dtype=str),
            Parameter(name='table', dtype=str)
        ]

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))

        download_step = DownloadStep(
            connector='fdi-data',
            connector_path='conns.yaml'
        )
        transform_step = TransformStep()
        load_step = LoadStep(
            params.get('table'), db_connector, if_exists='drop', 
            pk=[params.get('pk')], dtype=params.get('dtype'), 
            nullable_list=['count']
        )

        return [download_step, transform_step, load_step]