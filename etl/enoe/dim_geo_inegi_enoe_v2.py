import pandas as pd
from bamboo_lib.models import PipelineStep
from bamboo_lib.models import EasyPipeline
from bamboo_lib.connectors.models import Connector
from bamboo_lib.steps import DownloadStep, LoadStep

class ReadStep(PipelineStep):
    def run_step(self, prev, params):

        df = pd.read_csv(prev)
        df['cve_ent'] = df['cve_ent'].astype(str)
        df['cve_mun_full'] = df['cve_mun_full'].astype(str)
        df['cve_mun'] = df['cve_mun'].astype(str)

        return df

class DimEnoeGeoV2Pipeline(EasyPipeline):
    @staticmethod
    def steps(params, **kwargs):
        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))

        dtypes = {
            'nation_id':                'String',
            'nation_name':              'String', 
            'ent_id':                   'UInt8', 
            'ent_name':                 'String', 
            'ent_slug':                 'String', 
            'ent_iso3':                 'String', 
            'cve_ent':                  'String', 
            'mun_id':                   'UInt16', 
            'mun_name':                 'String', 
            'mun_slug':                 'String',
            'cve_mun':                  'String',
            'cve_mun_full':             'String', 
            'code':                     'UInt32'
        }

        download_step = DownloadStep(
            connector='enoe-geo-data',
            connector_path='conns.yaml'
        )

        read_step = ReadStep()
        load_step = LoadStep('dim_geo_inegi_enoe_v2', db_connector, dtype=dtypes,
                if_exists='drop', pk=['ent_id', 'mun_id', 'code'])
        
        return [download_step, read_step, load_step]

if __name__ == '__main__':
    pp = DimEnoeGeoV2Pipeline()
    pp.run({})