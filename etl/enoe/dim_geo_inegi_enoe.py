import pandas as pd
from bamboo_lib.models import EasyPipeline, Parameter, PipelineStep
from bamboo_lib.connectors.models import Connector
from bamboo_lib.steps import DownloadStep, LoadStep
from enoe_pipeline import TransformStep


class GeoStep(PipelineStep):
    def run_step(self, prev, params):
        logging.debug('GEO STEP')
        df = prev
        df = df[['code', 'mun_id']].copy()

        query = 'SELECT nation_id, nation_name, ent_id, ent_name, ent_slug, ent_iso3, cve_ent, mun_id, mun_name, mun_slug, cve_mun, cve_mun_full FROM dim_shared_geography_mun'
        geo = query_to_df(self.connector, raw_query=query)
        df = pd.merge(df, geo, on="mun_id", how="left")

        try:
            query = "SELECT * from dim_geo_inegi_enoe_v2"
            temp = query_to_df(self.connector, query)
            df = df.append(temp, sort=True).copy()
            df.drop_duplicates(subset=["code"], inplace=True)
        except Exception as e:
            print(e)
            None

        df['cve_ent'] = df['cve_ent'].astype(str)
        df['cve_mun_full'] = df['cve_mun_full'].astype(str)
        df['cve_mun'] = df['cve_mun'].astype(str)

        return df


class DimEnoeGeoV2Pipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return [
            Parameter(label="Year", name="year", dtype=str),
            Parameter(label="Quarter", name="quarter", dtype=str)
        ]

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
            'code':                     'String'
        }

        download_step = DownloadStep(
            connector=["enoe-1-data", "enoe-2-data", "social-data", "housing-data"],
            connector_path="conns.yaml"
        )

        transform_step = TransformStep()
        geo_step = GeoStep()
        load_step = LoadStep('dim_geo_inegi_enoe_v2', db_connector, dtype=dtypes,
                if_exists='drop', pk=['ent_id', 'mun_id', 'code'])
        
        return [download_step, transform_step, geo_step, load_step]

if __name__ == '__main__':
    pp = DimEnoeGeoV2Pipeline()
    pp.run({})