import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, PipelineStep, Parameter
from bamboo_lib.steps import LoadStep
from util import hs6_converter

class ReadStep(PipelineStep):
    def run_step(self, prev, params):
        # read data
        df = pd.read_csv(params.get('url'))
        df.columns = df.columns.str.lower()

        return df, params.get('url')

class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        df, url = prev[0], prev[1]
        # type conversion
        types = {
            'municipality_code': int,
            'product': str
        }
        for key,val in types.items():
            df[key] = df[key].astype(val)
        
        # columns creation
        df = df.loc[~(df['product'].str.len() == 4)].copy()
        df['product'] = df['product'].str.zfill(6)
        df['foreign_destination_origin'] = df['foreign_destination_origin'].str.lower()
        df['month_id'] = '20' + url[-6:-4] + url[-8:-6]
        df.value.replace('C', pd.np.nan, inplace=True)
        
        # hs codes
        for row in df['product'].unique():
            df['product'].replace(row, hs6_converter(row), inplace=True)
        
        # type conversion
        for col in df.columns[df.columns != 'foreign_destination_origin']:
            try:
                df[col] = df[col].astype('int')
            except:
                print('NaN values in column:', col)
                continue

        df.mun_id.replace(0, 50000, inplace=True)

        # rename columns
        names = {
            'municipality_code': 'mun_id',
            'foreign_destination_origin': 'partner_country',
            'trade_flow': 'flow_id'
        }
        df.rename(columns=names, inplace=True)

        return df

class ForeignTradePipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return [
            Parameter('url', dtype=str)
        ]

    @staticmethod
    def steps(params):
        
        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))
        
        dtype = {
            'month_id':                   'UInt32',
            'mun_id':                     'UInt16',
            'partner_country':            'String',
            'product':                    'UInt32',
            'flow_id':                    'UInt8',
            'value':                      'UInt32',
            'firms':                      'UInt16'
        }
        
        read_step = ReadStep()
        transform_step = TransformStep()
        load_step = LoadStep('economy_foreign_trade', db_connector, 
                             if_exists='append', pk=['month_id', 'mun_id', 'partner_country', 'product'], 
                             dtype=dtype, nullable_list=['value'])

        return [read_step, transform_step, load_step]