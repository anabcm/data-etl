import pandas as pd
from bamboo_lib.models import PipelineStep, AdvancedPipelineExecutor
from bamboo_lib.models import Parameter, EasyPipeline
from bamboo_lib.connectors.models import Connector
from bamboo_lib.steps import LoadStep
from bamboo_lib.helpers import query_to_df

class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        
        pk = params.get('pk_name')
        raw_query = 'SELECT {} FROM dim_shared_industry'.format(params.get('columns'))
        df = query_to_df(self.connector, raw_query=raw_query)
        df.drop_duplicates(subset=[pk], inplace=True)
        df[pk] = df[pk].astype(int)

        return df

class IndustryDimensions(EasyPipeline):
    @staticmethod
    def parameter_list():
        return [
            Parameter(label='Columns', name='columns', dtype=str),
            Parameter(label='PK_type', name='pk_type', dtype=str),
            Parameter(label='PK_name', name='pk_name', dtype=str)
        ]

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))

        dtype = {
            params.get('pk_name'): params.get('pk_type')
        }

        # Definition of each step
        transform_step = TransformStep(connector=db_connector)
        load_step = LoadStep("dim_shared_{}".format(params.get('pk_name').split('_id')[0]), db_connector, if_exists="drop", 
          pk=[params.get('pk_name')], dtype=dtype)
        
        return [transform_step, load_step]

if __name__ == "__main__":
    tables = {
        'industry_group_id': ['UInt16', 'industry_group_id,industry_group_es,industry_group_en,industry_group_es_short,industry_group_en_short'],
        'naics_industry_id': ['UInt32', 'naics_industry_id,naics_industry_es,naics_industry_en,naics_industry_es_short,naics_industry_en_short'],
        'national_industry_id': ['UInt32', 'national_industry_id,national_industry_es,national_industry_en,national_industry_es_short,national_industry_en_short']
    }

    pp = IndustryDimensions()

    for k, v in tables.items():
        pp.run({
            'pk_name': k,
            'pk_type': v[0],
            'columns': v[1],
        })
