
import numpy as np
import pandas as pd
from bamboo_lib.models import EasyPipeline, PipelineStep
from bamboo_lib.connectors.models import Connector
from bamboo_lib.steps import LoadStep, DownloadStep

class ReadStep(PipelineStep):
    def run_step(self, prev, params):
        df = pd.read_excel(prev, header=4)
        return df

class CleanStep(PipelineStep):
    def run_step(self, prev, params):

        df = prev

        df.columns = [x.strip().lower().replace(' ', '_') for x in df.columns]

        df.iloc[0, 0] = 'periodo'

        df = df.T.reset_index()

        df.loc[df['index'].str.contains('unnamed'), 'index'] = np.nan

        df['index'] = df['index'].ffill()

        df.columns = df.iloc[0]

        df = df.iloc[1::].copy()

        levels = ['11', '21', '22', '23', '31-33', '43', '46', 
                '48-49', '51', '52', '53', '54', '55', '56', 
                '61', '62', '71', '72', '81', '93']

        mask = []
        for ele in levels:
            for col in df.columns:
                try:            
                    if ' {} '.format(ele) in col:
                        mask.append(col)
                        continue
                except TypeError:
                    continue

        df = df.loc[:, ['concepto', 'periodo'] + mask].copy()

        df.dropna(inplace=True)

        df.columns = [x.strip().lower().replace(' ', '_') for x in df.columns]

        df = df.loc[df['periodo'].str.contains('T')].copy()

        df = df.melt(id_vars=['concepto', 'periodo'], var_name='sector_id')

        split = df['sector_id'].str.split('_-_', n=1, expand=True)
        df['sector_id'] = split[0]

        df.columns = ['quarter_id', 'quarter', 'sector_id', 'value']

        for exception in ['T', 'R', 'P']:
            df['quarter'] = df['quarter'].str.upper().str.replace(exception, '')
            df['quarter_id'] = df['quarter_id'].str.upper().str.replace(exception, '')

        df['quarter_id'] = df['quarter_id'] + df['quarter']
        df['quarter_id'] = df['quarter_id'].astype(int)

        df.drop(columns=['quarter'], inplace=True)

        return df

class GDPPipeline(EasyPipeline):
    @staticmethod
    def description():
        return 'ETL script for GDP, México'

    @staticmethod
    def website():
        return 'https://www.inegi.org.mx/contenidos/temas/economia/pib/pibt/tabulados/ori/PIBT_5.xlsx'

    @staticmethod
    def steps(params):

        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))

        dtype = {
            'sector_id':  'String',
            'quarter_id': 'UInt16',
            'value':      'UInt32'
        }

        download_step = DownloadStep(
            connector='gdp-data',
            connector_path='conns.yaml',
            force=True
        )

        read_step = ReadStep()
        clean_step = CleanStep()

        load_step = LoadStep('inegi_gdp', db_connector, if_exists='drop', pk=['quarter_id', 'sector_id'], dtype=dtype)

        return [download_step, read_step, clean_step, load_step]

if __name__ == "__main__":
    pp = GDPPipeline()
    pp.run({})