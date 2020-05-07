from datetime import datetime

import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.helpers import query_to_df
from bamboo_lib.models import EasyPipeline
from bamboo_lib.models import LoopHelper
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import LoadStep
from bamboo_lib.steps import UnzipToFolderStep
from bamboo_lib.steps import WildcardDownloadStep

from util import REVISION_MAP, hs6_converter


class ExtractStep(PipelineStep):
    def run_step(self, prev, params):
        list_of_results = prev

        if len(list_of_results) == 0:
            raise ValueError('No matches for command.')
        elif len(list_of_results) >= 2:
            raise ValueError('Too many matches for command.')

        return list_of_results[0][0]


class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        names = [
            'year', 'exporter', 'importer', 'hs_original_id',
            'value', 'quantity'
        ]

        year = params.get('year')
        hs_code = params.get('hs_code')

        df = pd.read_csv('{}BACI_HS{}_Y{}_V202001.csv'.format(prev, hs_code, year), header=0, names=names)

        # Trade value comes in thousands of USD
        df['value'] = df['value'] * 1000

        df['hs_original_id'] = df['hs_original_id'].astype(str).apply(lambda x: x.zfill(6))
        df['hs_master_id'] = df['hs_original_id'].apply(lambda x: int(hs6_converter(x)))

        # Converting exporter and importer IDs to ISO3 codes
        shared_countries_df = query_to_df(self.connector, 'select id_num, oec_id from dim_shared_countries where id_num is not null', ['id_num', 'oec_id'])
        id_num_oec_id_map = dict(zip(shared_countries_df['id_num'], shared_countries_df['oec_id']))

        clean_id_map = dict()

        for k, v in id_num_oec_id_map.items():
            for _id in k.split('|'):
                try:
                    clean_id_map[int(_id)] = v
                except ValueError:
                    # Skip NaN's
                    pass

        df['exporter'] = df['exporter'].replace(clean_id_map).astype(str)
        df['importer'] = df['importer'].replace(clean_id_map).astype(str)

        revision_name = 'hs{}'.format(params['hs_code'])

        # Get a the original HS6 name based on the revision_name
        shared_hs_df = query_to_df(self.connector, 'select hs6_id, hs6_name from dim_shared_{}'.format(revision_name), ['hs6_id', 'hs6_name'])
        hs_id_name_map = dict(zip(shared_hs_df['hs6_id'], shared_hs_df['hs6_name']))

        df['hs_original_name'] = df['hs_master_id'].replace(hs_id_name_map).astype(str)

        df['hs_revision'] = REVISION_MAP[revision_name]
        df['hs_revision'] = df['hs_revision'].astype(int)

        df = df[[
            'year', 'exporter', 'importer', 'hs_master_id', 'hs_revision',
            'hs_original_id', 'hs_original_name', 'value', 'quantity'
        ]]

        df['version'] = datetime.now()
        df['version'] = pd.to_datetime(df['version'], infer_datetime_format=True)

        return df


class BACIAnnualTradePipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return [
            Parameter(label="DB connector", name="db_connector", dtype=str, source=Connector),
            Parameter(label="HS Code", name="hs_code", dtype=str),
            Parameter(label="Year", name="year", dtype=str),
        ]

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch(params.get("db_connector"), open("../conns.yaml"))

        dtype = {
            'year':                 'UInt16',
            'hs_master_id':         'UInt32',
            'exporter':             'String',
            'importer':             'String',
            'value':                'Float64',
            'quantity':             'Float64',
            'version':              'DateTime',
        }

        download_step = WildcardDownloadStep(
            connector="baci-yearly",
            connector_path="conns.yaml"
        )
        extract_step = ExtractStep()
        unzip_step = UnzipToFolderStep(compression='zip', target_folder_path='temp/')
        transform_step = TransformStep(connector=db_connector)
        load_step = LoadStep(
            "trade_i_baci_a_{}".format(params['hs_code']), db_connector, if_exists="append", dtype=dtype,
            pk=['year', 'exporter', 'importer', 'hs_master_id'], nullable_list=['quantity'],
            engine="ReplacingMergeTree", engine_params='version'
        )

        return [download_step, extract_step, unzip_step, transform_step, load_step]


if __name__ == '__main__':
    years = list(range(2012, 2018 + 1))

    for year in years:
        pp = BACIAnnualTradePipeline()
        pp.run({
            'db_connector': "clickhouse-database",
            'hs_code': '12',
            'year': year,
        })