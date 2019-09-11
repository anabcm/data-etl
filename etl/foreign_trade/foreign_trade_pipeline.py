import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, PipelineStep, Parameter
from bamboo_lib.steps import LoadStep
from util import hs6_converter

class ReadStep(PipelineStep):
    def run_step(self, prev, params, base_url='https://storage.googleapis.com/datamexico-data/foreign_trade/'):
        # params
        level = params.get('level')
        period = params.get('period')
        depth_name = params.get('depth_name')
        month = params.get('month')
        year = params.get('year')
        # read data
        if period == 'Annual':
            url = ('{}{}/HS_{}/{}/{}_{}_annual{}.csv').format(base_url, level, depth_name, period, depth_name, level.lower(), year)
            df = pd.read_csv(url)

        elif level == 'State' and depth_name == '2D':
            try:
                url = ('{}{}/HS_{}/{}_{}_month{}{}.csv').format(base_url, level, depth_name, depth_name, level.lower(), month, year)
                df = pd.read_csv(url)
            except:
                url = ('{}{}/HS_{}/{}_{}_monthly{}{}.csv').format(base_url, level, depth_name, depth_name, level.lower(), month, year)
                df = pd.read_csv(url)
        elif period == 'Monthly' and level == 'State':
            try:
                url = ('{}{}/HS_{}/{}/{}_{}_month{}{}.csv').format(base_url, level, depth_name, period, depth_name, level.lower(), month, year)
                df = pd.read_csv(url)
            except:
                url = ('{}{}/HS_{}/{}/{}_{}_monthly{}{}.csv').format(base_url, level, depth_name, period, depth_name, level.lower(), month, year)
                df = pd.read_csv(url)
        elif period == 'Monthly' and level == 'Municipal':
            try:
                url = ('{}{}/HS_{}/{}/{}_{}_month{}{}.csv').format(base_url, level, depth_name, period, depth_name, level.lower()[:3], month, year)
                df = pd.read_csv(url)
            except:
                url = ('{}{}/HS_{}/{}/{}_{}_monthly{}{}.csv').format(base_url, level, depth_name, period, depth_name, level.lower()[:3], month, year)
                df = pd.read_csv(url)

        df.columns = df.columns.str.lower()
        
        return df, url

class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        df, url = prev[0], prev[1]
        # initial paramas
        params = {
            'depth': {'HS_2D': 2,
                    'HS_4D': 4,
                    'HS_6D': 6}
        }

        # get params
        for k, v in params['depth'].items():
            if k in url:
                depth = v
            else:
                df['hs' + str(v) + '_id'] = pd.np.nan

        # date/month
        if 'annual' in url:
            df.rename(columns={'date': 'year'}, inplace=True)
            df['month_id'] = pd.np.nan
        elif ('monthly' in url) or ('month' in url):
            df['month_id'] = '20' + url[-6:-4] + url[-8:-6]
            df['year'] = pd.np.nan
            df.drop(columns=['date'], inplace=True)
        
        # column name management
        for col in ['product_2d', 'product_4d']:
            df.rename(columns={col: 'product'}, inplace=True)

        # type conversion
        types = {
            'state_code': int,
            'municipality_code': int,
            'product': str
        }
        for key,val in types.items():
            try:
                df[key] = df[key].astype(val)
            except:
                continue

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
            'state_code': 'ent_id',
            'foreign_destination_origin': 'partner_country',
            'trade_flow': 'flow_id',
            'product': 'hs' + str(depth) + '_id'
        }
        df.rename(columns=names, inplace=True)

        # negative values
        df = df.loc[df.value > 0].copy()

        return df

class ForeignTradePipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return [
            Parameter('level', dtype=str),
            Parameter('depth_name', dtype=str),
            Parameter('depth_value', dtype=int),
            Parameter('period', dtype=str),
            Parameter('year', dtype=str),
            Parameter('month', dtype=str),
            Parameter('column_name', dtype=str),
        ]

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))
        
        dtype = {
            params.get('column_name')+'_id': 'UInt16',
            'hs2_id':                        'UInt16',
            'hs4_id':                        'UInt32',
            'hs6_id':                        'UInt32',
            'flow_id':                       'UInt8',
            'partner_country':               'String',   
            'firms':                         'UInt16',
            'value':                         'UInt32',           
            'month_id':                      'UInt32',
            'year':                          'UInt16'
        }
        
        read_step = ReadStep()
        transform_step = TransformStep()
        load_step = LoadStep('economy_foreign_trade_' + params.get('column_name'), db_connector, if_exists='append', pk=[params.get('column_name')+'_id', 'partner_country'], 
                             dtype=dtype, nullable_list=['hs2_id', 'hs4_id', 'hs6_id', 'value', 'month_id', 'year'])

        return [read_step, transform_step, load_step]