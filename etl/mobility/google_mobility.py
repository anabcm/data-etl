
import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import DownloadStep
from bamboo_lib.steps import LoadStep
from bamboo_lib.helpers import query_to_df

class TransformStep(PipelineStep):
    def run_step(self, prev, params):

        df = pd.read_csv('https://www.gstatic.com/covid19/mobility/Global_Mobility_Report.csv?cachebust=6d352e35dcffafce', keep_default_na=False, 
        na_values=["", "#N/A", "#N/A N/A", "#NA", "-1.#IND", "-1.#QNAN", "-NaN", "-nan", "1.#IND", "1.#QNAN", "<NA>", "N/A", "NULL", "NaN", "n/a", "nan", "null"])

        # replace countries iso2 -> iso3
        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))
        query = 'SELECT iso2, iso3 FROM dim_shared_country'
        db_countries = query_to_df(db_connector, raw_query=query)
        db_countries = dict(zip(db_countries['iso2'], db_countries['iso3']))
        df['country_region_code'] = df['country_region_code'].str.lower()
        df['country_region_code'].replace(db_countries, inplace=True)

        # filter columns
        cols = [x for x in df.columns if x not in ['country_region', 'sub_region_1', 'sub_region_2']]
        df = df.loc[:, cols].copy()

        # format date
        df['date'] = df['date'].str.replace('-', '').astype(int)

        df.rename(columns={
                'country_region_code': 'iso3',
                'date': 'date_id'
            }, inplace=True)
        
        for col in ['retail_and_recreation_percent_change_from_baseline',
                    'grocery_and_pharmacy_percent_change_from_baseline',
                    'parks_percent_change_from_baseline',
                    'transit_stations_percent_change_from_baseline',
                    'workplaces_percent_change_from_baseline',
                    'residential_percent_change_from_baseline']:
            df[col] = df[col].astype(float)

        return df

class GoogleMobility(EasyPipeline):
    @staticmethod
    def steps(params):
        db_connector = Connector.fetch("clickhouse-database", open("../conns.yaml"))

        dtype = {
            "iso3":                       "String",
            "date_id":                    "UInt32"
        }

        transform_step = TransformStep()
        load_step = LoadStep(
            "google_mobility", db_connector, if_exists="drop", pk=["iso3", "date_id"], dtype=dtype
        )

        return [transform_step, load_step]

if __name__ == "__main__":
    pp = GoogleMobility()
    pp.run({})