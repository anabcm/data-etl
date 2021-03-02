
import json
import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import Parameter, EasyPipeline, PipelineStep, Parameter
from bamboo_lib.steps import DownloadStep
from shared import get_dimensions, COUNTRY_REPLACE, SECTOR_REPLACE, LIMIT_C
from util import check_confidentiality
from static import FDI_COLUMNS


class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        data = prev
        
        result = {}
        historic = {}
        last_period = {}

        for sheet in params.get('sheets'):
            df = pd.read_excel(data, sheet_name=sheet)
            df.columns = df.columns.str.strip()
            df.rename(columns=FDI_COLUMNS, inplace=True)

            pk_id = [x for x in df.columns if x in ['sector_id', 'subsector_id', 'industry_group_id']][0]

            df.dropna(subset=[params.get('level')], inplace=True)

            # get end_id dimension
            dim_geo, dim_country = get_dimensions()

            if params.get('level') == 'ent_id':
                df['ent_id'].replace(dict(zip(dim_geo['ent_name'], dim_geo['ent_id'])), inplace=True)

            else:
                df['country_id'].replace(COUNTRY_REPLACE, inplace=True)
                df['country_id'].replace(dict(zip(dim_country['country_name_es'], dim_country['iso3'])), inplace=True)
                # filter "Otros países"
                df = df.loc[df['country_id'] != 'xxa'].copy()

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
                # top 3 acumulan mas IED 1999 - 2020
                temp = df.loc[df[pk_id] == ele, [params.get('level'), pk_id, 'value', 'count', 'value_c']] \
                    .groupby(by=[params.get('level'), pk_id]).sum().reset_index().sort_values(by=['value'], ascending=False)[:3]
                temp['top'] = range(1, temp.shape[0] + 1)

                # "C" values
                temp.loc[temp['count'] < LIMIT_C, 'value'] = 'C'

                top_3_historic = top_3_historic.append(temp, sort=False)

                # top 3 acumulan mas IED ultimo periodo
                temp = df.loc[df[pk_id] == ele, ['year', params.get('level'), pk_id, 'value', 'count']].groupby(by=['year', params.get('level'), pk_id]).sum().reset_index()
                temp = temp.loc[temp['year'] == temp['year'].max()].sort_values(by=['value'], ascending=False)[:3]
                temp['top'] = range(1, temp.shape[0] + 1)

                # "C" values
                temp.loc[temp['count'] < LIMIT_C, 'value'] = 'C'

                top_3_last_period = top_3_last_period.append(temp, sort=False)


            if params.get('level') == 'ent_id':
                top_3_historic['ent_name'] = top_3_historic['ent_id']
                top_3_historic['ent_name'].replace(dict(zip(dim_geo['ent_id'], dim_geo['ent_name'])), inplace=True)
                top_3_historic = top_3_historic[['ent_id', 'ent_name', pk_id, 'value', 'count', 'top']].copy()

                top_3_last_period['ent_name'] = top_3_last_period['ent_id']
                top_3_last_period['ent_name'].replace(dict(zip(dim_geo['ent_id'], dim_geo['ent_name'])), inplace=True)
                top_3_last_period = top_3_last_period[['ent_id', 'ent_name', pk_id, 'value', 'count', 'year']].copy()

            else:
                top_3_historic['country_name'] = top_3_historic['country_id']
                top_3_historic['country_name'].replace(dict(zip(dim_country['iso3'], dim_country['country_name_es'])), inplace=True)
                top_3_historic['country_name_en'] = top_3_historic['country_id']
                top_3_historic['country_name_en'].replace(dict(zip(dim_country['iso3'], dim_country['country_name'])), inplace=True)
                top_3_historic = top_3_historic[['country_id', 'country_name', 'country_name_en', pk_id, 'value', 'count', 'top']].copy()

                top_3_last_period['country_name'] = top_3_last_period['country_id']
                top_3_last_period['country_name'].replace(dict(zip(dim_country['iso3'], dim_country['country_name_es'])), inplace=True)
                top_3_last_period['country_name_en'] = top_3_last_period['country_id']
                top_3_last_period['country_name_en'].replace(dict(zip(dim_country['iso3'], dim_country['country_name'])), inplace=True)
                top_3_last_period = top_3_last_period[['country_id', 'country_name', 'country_name_en', pk_id, 'value', 'count', 'year']].copy()

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
            Parameter(name='level', dtype=str),
            Parameter(name='sheets', dtype=list),
            Parameter(name='dim', dtype=str),
            Parameter(name='file_name', dtype=str),
            Parameter(name='source', dtype=str)
        ]

    @staticmethod
    def steps(params):
        
        download_step = DownloadStep(
            connector=params.get('source'),
            connector_path='conns.yaml',
            force=True
        )

        transform_step = TransformStep()

        return [download_step, transform_step]

if __name__ == "__main__":
    pp = FDIaggregatePipeline()
    for level, sheets in {'ent_id': [['7', '8', '9'], 'top3_ent', "fdi-data-additional"], 
                          'country_id': [['4', '5', '6'], 'top3_country', "fdi-data-additional"]}.items():
        df = pp.run({
            'level': level,
            'sheets': sheets[0],
            'file_name': sheets[1],
            'source': sheets[2]
        })