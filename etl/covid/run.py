
import os
import pandas as pd
from bamboo_lib.helpers import query_to_df
from bamboo_lib.connectors.models import Connector

os.system('bamboo-cli --folder . --entry covid_pipeline')

db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))
min_query = 'SELECT min(ingress_date), min(symptoms_date), min(death_date), min(updated_date) FROM gobmx_covid'
max_query = 'SELECT max(ingress_date), max(symptoms_date), max(death_date), max(updated_date) FROM gobmx_covid'
min_value = str(min(query_to_df(db_connector, raw_query=min_query).iloc[0].to_list()))
max_value = str(max(query_to_df(db_connector, raw_query=max_query).iloc[0].to_list()))
min_time = '{}-{}-{}'.format(min_value[:4], min_value[4:6], min_value[6:])
max_time = '{}-{}-{}'.format(max_value[:4], max_value[4:6], max_value[6:])
os.system('bamboo-cli --folder . --entry dim_time_date --init={} --end={}'.format(min_time, max_time))