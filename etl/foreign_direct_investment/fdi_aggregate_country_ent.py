
import json
import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import Parameter, EasyPipeline, PipelineStep, Parameter
from bamboo_lib.steps import DownloadStep
from shared import get_dimensions, COUNTRY_REPLACE


class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        data = prev

        result = {}
        historic = {}
        last_period = {}

        for sheet in params.get('sheets'):
            df = pd.read_excel(data, sheet_name=sheet)

            df.rename(columns={
                'Año': 'year',
                'Entidad federativa': 'ent_id',
                'País de Origen DEAE': 'country_id',
                'País': 'country_id',
                'Subsector': 'subsector_id',
                'Sector': 'sector_id',
                'Rama': 'industry_group_id',
                'Monto': 'value',
                'Recuento': 'count',
                'Monto C': 'value_c'
            }, inplace=True)

            pk_id = 'ent_id'

            df.dropna(subset=['country_id'], inplace=True)
            df['year'] = df['year'].astype(int)

            # get end_id dimension
            dim_geo, dim_country = get_dimensions()

            df['ent_id'].replace(dict(zip(dim_geo['ent_name'], dim_geo['ent_id'])), inplace=True)

            df['country_id'].replace(COUNTRY_REPLACE, inplace=True)
            df['country_id'].replace(dict(zip(dim_country['country_name_es'], dim_country['iso3'])), inplace=True)

            temp = pd.DataFrame()
            top_3_historic = pd.DataFrame()
            top_3_last_period = pd.DataFrame()

            for ele in list(df['ent_id'].unique()):
                # top 3 acumulan mas IED 1999 - 2020
                temp = df.loc[df['ent_id'] == ele, ['year', 'country_id', 'ent_id', 'value', 'count']] \
                    .groupby(by=['ent_id', 'country_id']).sum().reset_index().sort_values(by=['value'], ascending=False)[:3]
                temp['top'] = range(1, temp.shape[0] + 1)
                
                # 
                # "C" values
                temp.loc[temp['count'] < 3, 'value'] = 'C'
                
                top_3_historic = top_3_historic.append(temp, sort=False)

                # top 3 acumulan mas IED ultimo periodo
                temp = df.loc[df[pk_id] == ele, ['year', 'country_id', 'ent_id', 'value', 'count']].groupby(by=['year', 'ent_id', 'country_id']).sum().reset_index()
                temp = temp.loc[temp['year'] == temp['year'].max()].sort_values(by=['value'], ascending=False)[:3]
                temp['top'] = range(1, temp.shape[0] + 1)
                
                # "C" values
                temp.loc[temp['count'] < 3, 'value'] = 'C'

                top_3_last_period = top_3_last_period.append(temp, sort=False)

                
            top_3_historic['ent_name'] = top_3_historic['ent_id']
            top_3_historic['ent_name'].replace(dict(zip(dim_geo['ent_id'], dim_geo['ent_name'])), inplace=True)
            top_3_historic['country_name'] = top_3_historic['country_id']
            top_3_historic['country_name'].replace(dict(zip(dim_country['iso3'], dim_country['country_name_es'])), inplace=True)
            top_3_historic['country_name_en'] = top_3_historic['country_id']
            top_3_historic['country_name_en'].replace(dict(zip(dim_country['iso3'], dim_country['country_name'])), inplace=True)
            top_3_historic = top_3_historic[['ent_id', 'ent_name', 'country_id', 'country_name', 'country_name_en', 'value', 'count', 'top']].copy()

            top_3_last_period['ent_name'] = top_3_last_period['ent_id']
            top_3_last_period['ent_name'].replace(dict(zip(dim_geo['ent_id'], dim_geo['ent_name'])), inplace=True)
            top_3_last_period['country_name'] = top_3_last_period['country_id']
            top_3_last_period['country_name'].replace(dict(zip(dim_country['iso3'], dim_country['country_name_es'])), inplace=True)
            top_3_last_period['country_name_en'] = top_3_last_period['country_id']
            top_3_last_period['country_name_en'].replace(dict(zip(dim_country['iso3'], dim_country['country_name'])), inplace=True)
            top_3_last_period = top_3_last_period[['ent_id', 'ent_name', 'country_id', 'country_name', 'country_name_en', 'value', 'count', 'year']].copy()

            historic[pk_id.split('_id')[0]] = top_3_historic.to_dict(orient='records')
            last_period[pk_id.split('_id')[0]] = top_3_last_period.to_dict(orient='records')

        result['top3_industry_ent_historic'] = historic
        result['top3_industry_ent_last_period'] = last_period

        with open('{}.json'.format(params.get('file_name')), 'w') as outfile:
            json.dump(result, outfile)

        return df

class FDIaggregatePipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return [
            Parameter(name='sheets', dtype=list),
            Parameter(name='file_name', dtype=str),
            Parameter(name='source', dtype=str)
        ]

    @staticmethod
    def steps(params):

        download_step = DownloadStep(
            connector=params.get('source'),
            connector_path="conns.yaml"
        )

        transform_step = TransformStep()

        return [download_step, transform_step]


if __name__ == "__main__":
    pp = FDIaggregatePipeline()
    for file_name, sheets in {'country_ent': [['3'], 'fdi-data']}.items():
        df = pp.run({
            'source': sheets[1],
            'sheets': sheets[0],
            'file_name': file_name
        })