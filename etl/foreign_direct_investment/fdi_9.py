import pandas as pd	
from bamboo_lib.connectors.models import Connector	
from bamboo_lib.models import EasyPipeline	
from bamboo_lib.models import Parameter	
from bamboo_lib.models import PipelineStep	
from bamboo_lib.steps import DownloadStep	
from bamboo_lib.steps import LoadStep	
from helpers import norm	
from shared import INVESTMENT_TYPE	


class Transform_92_Step(PipelineStep):	
    def run_step(self, prev, params):	
        data = prev	

        df = pd.read_excel(data, sheet_name='9.2')	
        df.columns = [norm(x.strip().lower().replace(' ', '_').replace('-', '_').replace('%', 'perc')) for x in df.columns]	

        df.columns = ['year', 'value', 'count', 'value_c']	

        df = df.loc[~df['year'].str.contains('Total')].copy()	

        df.drop(columns=['value'], inplace=True)	

        for col in ['count', 'year']:	
            df[col] = df[col].astype(int)	

        return df	

class Transform_93_Step(PipelineStep):	
    def run_step(self, prev, params):	
        data = prev	

        df = pd.read_excel(data, sheet_name='9.3')	
        df.columns = [norm(x.strip().lower().replace(' ', '_').replace('-', '_').replace('%', 'perc')) for x in df.columns]	

        df.columns = ['year', 'quarter', 'value', 'count', 'value_c']	

        df = df.loc[~df['year'].str.contains('Total')].copy()	

        df['quarter_id'] = df['year'].astype(str) + df['quarter'].astype(int).astype(str)	

        df.drop(columns=['value', 'year', 'quarter'], inplace=True)	

        for col in ['count', 'quarter_id']:	
            df[col] = df[col].astype(float).astype(int)	

        return df	

class Transform_94_Step(PipelineStep):	
    def run_step(self, prev, params):	
        data = prev	

        df = pd.read_excel(data, sheet_name='9.4')	
        df.columns = [norm(x.strip().lower().replace(' ', '_').replace('-', '_').replace('%', 'perc')) for x in df.columns]	

        df.columns = ['year', 'value_between_companies', 'value_new_investments', 'value_re_investments', 	
                    'count_between_companies', 'count_new_investments', 'count_re_investments',	
                    'value_between_companies_c', 'value_new_investments_c', 'value_re_investments_c']	

        df = df.loc[~df['year'].str.contains('Total')].copy()	

        df.drop(columns=['value_between_companies', 'value_new_investments', 'value_re_investments'], inplace=True)	

        for col in df.columns:	
            df[col] = df[col].astype(float)	

        base = ['year']	
        df_final = pd.DataFrame()	
        for option in ['between_companies', 'new_investments', 're_investments']:	
            temp = df[base + ['count_{}'.format(option), 'value_{}_c'.format(option)]]	
            temp.columns = ['year', 'count', 'value_c']	
            temp.dropna(subset=['value_c'], inplace=True)	
            temp['investment_type'] = option	
            df_final = df_final.append(temp)	
        df = df_final.copy()	

        df['investment_type'].replace(INVESTMENT_TYPE, inplace=True)	

        return df	

class Transform_95_Step(PipelineStep):	
    def run_step(self, prev, params):	
        data = prev	

        df = pd.read_excel(data, sheet_name='9.5')	
        df.columns = [norm(x.strip().lower().replace(' ', '_').replace('-', '_').replace('%', 'perc')) for x in df.columns]	

        df.columns = ['year', 'quarter', 'value_between_companies', 'value_new_investments', 'value_re_investments', 	
                    'count_between_companies', 'count_new_investments', 'count_re_investments',	
                    'value_between_companies_c', 'value_new_investments_c', 'value_re_investments_c']	

        df = df.loc[~df['year'].str.contains('Total')].copy()	

        df['quarter_id'] = df['year'].astype(str) + df['quarter'].astype(int).astype(str)	

        df.drop(columns=['year', 'quarter', 'value_between_companies', 'value_new_investments', 'value_re_investments'], inplace=True)	

        for col in df.columns:	
            df[col] = df[col].astype(float)	

        base = ['quarter_id']	
        df_final = pd.DataFrame()	
        for option in ['between_companies', 'new_investments', 're_investments']:	
            temp = df[base + ['count_{}'.format(option), 'value_{}_c'.format(option)]]	
            temp.columns = ['quarter_id', 'count', 'value_c']	
            temp.dropna(subset=['value_c'], inplace=True)	
            temp['investment_type'] = option	
            df_final = df_final.append(temp)	
        df = df_final.copy()	


        df['investment_type'].replace(INVESTMENT_TYPE, inplace=True)	

        return df	

class FDI9Pipeline(EasyPipeline):	
    @staticmethod	
    def parameter_list():	
        return [	
            Parameter(name='table', dtype=float)	
        ]	

    @staticmethod	
    def steps(params):	
        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))	

        download_step = DownloadStep(	
            connector='fdi-data',	
            connector_path='conns.yaml'	
        )	

        transform_92_step = Transform_92_Step()	
        transform_93_step = Transform_93_Step()	
        transform_94_step = Transform_94_Step()	
        transform_95_step = Transform_95_Step()	

        if params.get('table') == 9.2:	

            load_step = LoadStep('fdi_9_year', db_connector, if_exists='drop', 	
                    pk=['year'], dtype={'year': 'UInt16',	
                                        'count': 'UInt16',	
                                        'value_c': 'Float32'})	

            return [download_step, transform_92_step, load_step]	

        if params.get('table') == 9.3:	

            load_step = LoadStep('fdi_9_quarter', db_connector, if_exists='drop', 	
                    pk=['quarter_id'], dtype={'quarter_id': 'UInt16',	
                                              'count': 'UInt16',	
                                              'value_c': 'Float32'})	

            return [download_step, transform_93_step, load_step]	

        if params.get('table') == 9.4:	

            load_step = LoadStep('fdi_9_year_investment', db_connector, if_exists='drop', 	
                    pk=['year'], dtype={'year': 'UInt16',	
                                        'investment_type':  'UInt8',	
                                        'count':            'UInt16',	
                                        'value_c':          'Float32'})	

            return [download_step, transform_94_step, load_step]	

        if params.get('table') == 9.5:	

            load_step = LoadStep('fdi_9_quarter_investment', db_connector, if_exists='drop', 	
                    pk=['quarter_id'], dtype={'quarter_id': 'UInt16',	
                                              'investment_type':  'UInt8',	
                                              'count':            'UInt16',	
                                              'value_c':          'Float32'})	

            return [download_step, transform_95_step, load_step]	

if __name__ == '__main__':	
    pp = FDI9Pipeline()	
    for i in [9.2, 9.3, 9.4, 9.5]:	
        pp.run({'table': i}) 