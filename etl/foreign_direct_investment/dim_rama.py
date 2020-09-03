
import pandas as pd
from bamboo_lib.models import PipelineStep, EasyPipeline
from bamboo_lib.connectors.models import Connector
from bamboo_lib.steps import LoadStep
from bamboo_lib.helpers import query_to_df


class TransformStep(PipelineStep):
    def run_step(self, prev, params):

        dim_industry_query = 'SELECT sector_id, sector_es, sector_en, subsector_id, subsector_es, subsector_en, \
                             industry_group_id, industry_group_es, industry_group_en FROM dim_shared_industry_fdi'
        df = query_to_df(self.connector, raw_query=dim_industry_query)
        df.drop_duplicates(subset='industry_group_id', inplace=True)

        return df

class FDIIndustryPipeline(EasyPipeline):
    @staticmethod
    def steps(params, **kwargs):
        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))

        dtypes = {
            'sector_id':          'UInt8',
            'subsector_id':       'UInt16',
            'industry_group_id':  'UInt16'
        }

        transform_step = TransformStep(connector=db_connector)
        load_step = LoadStep('dim_shared_subsector_fdi', db_connector, dtype=dtypes,
                if_exists='drop', pk=['sector_id', 'subsector_id', 'industry_group_id'])
        
        return [transform_step, load_step]

if __name__ == '__main__':
    pp = FDIIndustryPipeline()
    pp.run({})