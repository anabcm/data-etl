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
        # initial paramas
        params = {
            'period': ['Annual', 'Monthly'],
            'depth': {'HS_2D': 2,
                    'HS_4D': 4,
                    'HS_6D': 6}
        }

        # get params
        for ele in params['period']:
            if ele in url:
                period = ele
                break
        for k, v in params['depth'].items():
            if k in url:
                depth = v
            else:
                df['hs_0' + str(v)] = pd.np.nan

        # date/month
        if period == 'Annual':
            df.rename(columns={'date': 'year'}, inplace=True)
            df['month_id'] = pd.np.nan
        elif period == 'Monthly':
            df['month_id'] = '20' + url[-6:-4] + url[-8:-6]
            df['year'] = pd.np.nan
            df.drop(columns=['date'], inplace=True)
        
        for col in ['product_2d', 'product_4d']:
            df.rename(columns={col: 'product'}, inplace=True)

        # type conversion
        types = {
            'municipality_code': int,
            'product': str
        }
        for key,val in types.items():
            df[key] = df[key].astype(val)

        # special case
        if depth == 6:
            df.loc[df['product'].str.len() == 4, 'product'] = df.loc[df['product'].str.len() == 4, 'product'] + '00'
        if depth == 4:
            df.loc[df['product'].str.len() == 2, 'product'] = df.loc[df['product'].str.len() == 2, 'product'] + '00'

        df['product'] = df['product'].str.zfill(depth)

        # iso3 names
        df['foreign_destination_origin'] = df['foreign_destination_origin'].str.lower()

        df.value.replace('C', pd.np.nan, inplace=True)

        # hs codes
        for row in df['product'].unique():
            df['product'].replace(row, hs6_converter(row), inplace=True)

        # type conversion
        for col in df.columns[df.columns != 'foreign_destination_origin']:
            try:
                df[col] = df[col].astype('float')
            except:
                print('NaN values in column:', col)
                continue

        # rename columns
        names = {
            'municipality_code': 'mun_id',
            'foreign_destination_origin': 'partner_country',
            'trade_flow': 'flow_id',
            'product': 'hs' + str(depth) + '_id'
        }
        df.rename(columns=names, inplace=True)

        # muncipality '0'
        df.mun_id.replace(0, 50000, inplace=True)

        # order
        df = df[['mun_id', 'hs2_id', 'hs4_id', 'hs6_id', 'flow_id', 'partner_country', 'firms', 'value', 'month_id', 'year']].copy()

        # negative values
        df = df.loc[df.value > 0].copy()

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
            'mun_id':                     'UInt16',
            'hs2_id':                     'UInt16',
            'hs4_id':                     'UInt32',
            'hs6_id':                     'UInt32',
            'flow_id':                    'UInt8',
            'partner_country':            'String',   
            'firms':                      'UInt16',
            'value':                      'UInt32',           
            'month_id':                   'UInt32',
            'year':                       'UInt16'
        }
        
        read_step = ReadStep()
        transform_step = TransformStep()
        load_step = LoadStep('economy_foreign_trade_mun', db_connector, 
                             if_exists='append', pk=['mun_id', 'partner_country'], 
                             dtype=dtype, nullable_list=['hs_02', 'hs_04', 'hs_06',
                                                        'value', 'month_id', 'year'])

        return [read_step, transform_step, load_step]