
import os
import pandas as pd
from covid_pipeline import CovidPipeline
from dim_time_date import DimTimeDatePipeline
from bamboo_lib.helpers import query_to_df
from bamboo_lib.connectors.models import Connector

db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))
min_query = 'SELECT min(ingress_date), min(symptoms_date), min(updated_date) FROM gobmx_covid'
max_query = 'SELECT max(ingress_date), max(symptoms_date), max(updated_date) FROM gobmx_covid'
min_value = str(min(query_to_df(db_connector, raw_query=min_query).iloc[0].to_list()))
max_value = str(max(query_to_df(db_connector, raw_query=max_query).iloc[0].to_list()))
min_time = '{}-{}-{}'.format(min_value[:4], min_value[4:6], min_value[6:])
max_time = '{}-{}-{}'.format(max_value[:4], max_value[4:6], max_value[6:])

pp = DimTimeDatePipeline()
pp.run({'init': min_time,
        'end': max_time})