
import json
import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import Parameter, EasyPipeline, PipelineStep, Parameter
from bamboo_lib.steps import DownloadStep
from shared import get_dimensions, SECTOR_REPLACE, COUNTRY_REPLACE, LIMIT_C


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

            pk_id = [x for x in df.columns if x in ['country_id', 'sector_id', 'subsector_id', 'industry_group_id']][0]

            df.dropna(subset=[pk_id], inplace=True)

            # get end_id dimension
            _dim_geo, dim_country = get_dimensions()

            if pk_id != 'country_id':
                split = df[pk_id].str.split(' ', n=1, expand=True)
                df[pk_id] = split[0]
                df[pk_id] = df[pk_id].astype(int)

                if pk_id == 'sector_id':
                    df['sector_id'].replace(SECTOR_REPLACE, inplace=True)
                    df['sector_id'] = df['sector_id'].astype(str)
            
            else:
                df['country_id'].replace(COUNTRY_REPLACE, inplace=True)
                df['country_id'].replace(dict(zip(dim_country['country_name_es'], dim_country['iso3'])), inplace=True)

            temp = pd.DataFrame()
            top_3_historic = pd.DataFrame()
            top_3_last_period = pd.DataFrame()

            for ele in list(df[pk_id].unique()):
                # top 3 acumulan mas IED 1999 - 2020
                temp = df.loc[df[pk_id] == ele, [pk_id, 'value', 'count']] \
                    .groupby(by=[pk_id]).sum().reset_index().sort_values(by=['value'], ascending=False)

                top_3_historic = top_3_historic.append(temp, sort=False)

                # top 3 acumulan mas IED ultimo periodo
                temp = df.loc[df[pk_id] == ele, ['year', pk_id, 'value', 'count']].groupby(by=['year', pk_id]).sum().reset_index()
                temp = temp.loc[(temp[pk_id] == ele) & (temp['year'] == temp['year'].max()), ['year', pk_id, 'value', 'count']] \
                    .groupby(by=['year', pk_id]).sum().reset_index().sort_values(by=['value'], ascending=False)

                top_3_last_period = top_3_last_period.append(temp, sort=False)

            if pk_id == 'country_id':
                top_3_historic.sort_values(by=['value'], ascending=False, inplace=True)
                top_3_historic = top_3_historic[:3]
                # "C" values
                top_3_historic.loc[top_3_historic['count'] < LIMIT_C, 'value'] = 'C'
                top_3_historic['country_name'] = top_3_historic['country_id']
                top_3_historic['country_name'].replace(dict(zip(dim_country['iso3'], dim_country['country_name_es'])), inplace=True)
                top_3_historic['country_name_en'] = top_3_historic['country_id']
                top_3_historic['country_name_en'].replace(dict(zip(dim_country['iso3'], dim_country['country_name'])), inplace=True)

                top_3_last_period.sort_values(by=['value'], ascending=False, inplace=True)
                top_3_last_period = top_3_last_period[:3]
                # "C" values
                top_3_last_period.loc[top_3_last_period['count'] < LIMIT_C, 'value'] = 'C'
                top_3_last_period['country_name'] = top_3_last_period['country_id']
                top_3_last_period['country_name'].replace(dict(zip(dim_country['iso3'], dim_country['country_name_es'])), inplace=True)
                top_3_last_period['country_name_en'] = top_3_last_period['country_id']
                top_3_last_period['country_name_en'].replace(dict(zip(dim_country['iso3'], dim_country['country_name'])), inplace=True)

                top_3_historic['top'] = range(1, top_3_historic.shape[0] + 1)
                top_3_last_period['top'] = range(1, top_3_last_period.shape[0] + 1)
            else:
                # "C" values
                top_3_historic.loc[top_3_historic['count'] < LIMIT_C, 'value'] = 'C'
                top_3_last_period.loc[top_3_last_period['count'] < LIMIT_C, 'value'] = 'C'

            historic[pk_id.split('_id')[0]] = top_3_historic.to_dict(orient='records')
            last_period[pk_id.split('_id')[0]] = top_3_last_period.to_dict(orient='records')

        if pk_id == 'country_id':
            result['nation_accumulated_historic_IED'] = historic
            result['nation_accumulated_last_period_IED'] = last_period
        else:
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
    for file_name, sheets in {'accumulated_value': [['1', '2', '3'], 'fdi-data-additional-3'],
                              'top3_country_nation': [['10.1'], 'fdi-data']}.items():
        df = pp.run({
            'source': sheets[1],
            'sheets': sheets[0],
            'file_name': file_name
        })