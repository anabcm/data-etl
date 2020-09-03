
import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.helpers import query_to_df

def get_dimensions():
    db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))
    dim_geo_query = 'SELECT ent_name, ent_id FROM dim_shared_geography_ent'
    dim_geo = query_to_df(db_connector, raw_query=dim_geo_query)

    dim_country_query = 'SELECT country_name_es, iso3 FROM dim_shared_country'
    dim_country = query_to_df(db_connector, raw_query=dim_country_query)

    return [dim_geo, dim_country]