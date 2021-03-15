
"""Ingest all files"""

import os
import glob
import pandas as pd
from dim_time_date import DimTimeDatePipeline
from dim_time_covid_stats import DimTimeDateCovidStatsPipeline
from bamboo_lib.helpers import query_to_df
from bamboo_lib.connectors.models import Connector

data = sorted(glob.glob('*.csv'))

#for file_path in data[-5::]:
#    os.system('bamboo-cli --folder . --entry covid_pipeline --file_path="{}"'.format(file_path))

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

pp = DimTimeDateCovidStatsPipeline()
pp.run({'init': min_time,
        'end': max_time})