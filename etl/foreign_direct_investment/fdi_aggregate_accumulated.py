
import glob
import json
import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import Parameter, EasyPipeline, PipelineStep, Parameter
from bamboo_lib.steps import DownloadStep
from shared import SECTOR_REPLACE
#from util import fill_levels
#from util import check_confidentiality
#from etl.foreign_direct_investment.shared import get_dimensions, COUNTRY_REPLACE, SECTOR_REPLACE

def check_confidentiality_no_geo(df, industry, industry_pk, confidential_column, confidential_value, value):
    query = list(df.loc[df[industry_pk] == industry, confidential_column])
    try:
        test = query.count('C')/len(query)
    except ZeroDivisionError:
        test = 0
    if test > 0.1:
        return 'C'
    else:
        return value


class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        data = prev

        result = {}
        #historic = pd.DataFrame()
        #last_period = pd.DataFrame()
        historic = {}
        last_period = {}

        for sheet in params.get('sheets'):
            df = pd.read_excel(data, sheet_name=sheet)

            df.rename(columns={
                'Año': 'year',
                'Entidad federativa': 'ent_id',
                'País de Origen DEAE': 'country_id',
                'Subsector': 'subsector_id',
                'Sector': 'sector_id',
                'Rama': 'industry_group_id',
                'Monto': 'value',
                'Recuento': 'count',
                'Monto C': 'value_c'
            }, inplace=True)

            pk_id = [x for x in df.columns if x in ['sector_id', 'subsector_id', 'industry_group_id']][0]

            df.dropna(subset=[pk_id], inplace=True)

            # get end_id dimension
            #dim_geo, dim_country = get_dimensions()

            """if params.get('level') == 'ent_id':
                df['ent_id'].replace(dict(zip(dim_geo['ent_name'], dim_geo['ent_id'])), inplace=True)

            else:
                df['country_id'].replace(COUNTRY_REPLACE, inplace=True)
                df['country_id'].replace(dict(zip(dim_country['country_name_es'], dim_country['iso3'])), inplace=True)"""

            split = df[pk_id].str.split(' ', n=1, expand=True)
            df[pk_id] = split[0]
            df[pk_id] = df[pk_id].astype(int)

            if pk_id == 'sector_id':
                df['sector_id'].replace(SECTOR_REPLACE, inplace=True)
                df['sector_id'] = df['sector_id'].astype(str)

            temp = pd.DataFrame()
            top_3_historic = pd.DataFrame()
            top_3_last_period = pd.DataFrame()

            for ele in list(df[pk_id].unique()):
                # top 3 entidades federativas que acumulan mas IED 1999 - 2020
                temp = df.loc[df[pk_id] == ele, ['year', pk_id, 'value', 'count']].groupby(by=[pk_id]).sum().reset_index()
                temp = temp.sort_values(by=['value'], ascending=False)
                #temp = temp.sort_values(by=['value'], ascending=False)[:3]
                #temp['top'] = range(1, temp.shape[0] + 1)
                # temp['indicador'] = 1
                ## fill levels to append
                """temp = fill_levels(temp, pk_id)
                temp['year'] = 0"""
                for item in temp.iterrows():
                    temp.loc[temp[pk_id] == item[1][pk_id], 'value'] = \
                        check_confidentiality_no_geo(df, ele, pk_id, 'value_c', 'C',  item[1]['value'])
                top_3_historic = top_3_historic.append(temp, sort=False)

                # top 3 entidades federativas que acumulan mas IED ultimo anio
                temp = df.loc[df[pk_id] == ele, ['year', pk_id, 'value', 'count']].groupby(by=['year', pk_id]).sum().reset_index()
                temp = temp.loc[temp['year'] == temp['year'].max()].sort_values(by=['value'], ascending=False)
                #temp = temp.loc[temp['year'] == temp['year'].max()].sort_values(by=['value'], ascending=False)[:3]
                #temp['top'] = range(1, temp.shape[0] + 1)
                # temp['indicador'] = 2
                for item in temp.iterrows():
                    temp.loc[(temp['year'] == item[1]['year']) & (temp[pk_id] == item[1][pk_id]), 'value'] = \
                        list(df.loc[(df['year'] == item[1]['year']) & (df[pk_id] == item[1][pk_id]), 'value_c'])[0]
                ## fill levels to append
                #temp = fill_levels(temp, pk_id)
                top_3_last_period = top_3_last_period.append(temp, sort=False)

            historic[pk_id.split('_id')[0]] = top_3_historic.to_dict(orient='records')
            last_period[pk_id.split('_id')[0]] = top_3_last_period.to_dict(orient='records')

        result['accumulated_historic_IED'] = historic
        result['accumulated_last_period_IED'] = last_period

        with open('{}.json'.format(params.get('file_name')), 'w') as outfile:
            json.dump(result, outfile)

        return df

class FDIaggregatePipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return [
            Parameter(name='sheets', dtype=list),
            Parameter(name='file_name', dtype=str)
        ]

    @staticmethod
    def steps(params):
        #db_connector = Connector.fetch("clickhouse-database", open("../conns.yaml"))

        """dtype = {
            'year':              'UInt16',
            'ent_id':            'UInt8',
            'country_id':        'String',
            'sector_id':         'String',
            'subsector_id':      'UInt16',
            'industry_group_id': 'UInt16',
            'top':               'UInt8',
            'indicador':         'UInt8',
            'value':             'Float32',
            'count':             'UInt16'
        }"""

        download_step = DownloadStep(
            connector="fdi-data-additional-3",
            connector_path="conns.yaml"
        )

        transform_step = TransformStep()

        return [download_step, transform_step]


if __name__ == "__main__":
    pp = FDIaggregatePipeline()
    for file_name, sheets in {'accumulated_value': ['1', '2', '3']}.items():
        df = pp.run({
            'sheets': sheets,
            'file_name': file_name
        })