
import json
import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import Parameter, EasyPipeline, PipelineStep, Parameter
from bamboo_lib.steps import DownloadStep
from shared import get_dimensions, COUNTRY_REPLACE, SECTOR_REPLACE
#from util import fill_levels
from util import check_confidentiality

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
                'Tipo de inversión': 'investment_type',
                'Subsector': 'subsector_id',
                'Sector': 'sector_id',
                'Rama': 'industry_group_id',
                'Monto': 'value',
                'Recuento': 'count',
                'Monto C': 'value_c'
            }, inplace=True)

            pk_id = [x for x in df.columns if x in ['sector_id', 'subsector_id', 'industry_group_id']][0]

            df.dropna(subset=[params.get('level')], inplace=True)

            """# get end_id dimension
            dim_geo, dim_country = get_dimensions()

            if params.get('level') == 'ent_id':
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
                temp = df.loc[df[pk_id] == ele, [params.get('level'), pk_id, 'value', 'count', 'value_c']] \
                    .groupby(by=[params.get('level'), pk_id]).sum().reset_index().sort_values(by=['value'], ascending=False)

                # "C" values
                temp.loc[temp['count'] <= 3, 'value'] = 'C'

                top_3_historic = top_3_historic.append(temp, sort=False)

                # top 3 entidades federativas que acumulan mas IED ultimo anio
                temp = df.loc[df[pk_id] == ele, ['year', params.get('level'), pk_id, 'value', 'count']].groupby(by=['year', params.get('level'), pk_id]).sum().reset_index()
                temp = temp.loc[temp['year'] == temp['year'].max()].sort_values(by=['value'], ascending=False)

                # "C" values
                temp.loc[temp['count'] <= 3, 'value'] = 'C'

                top_3_last_period = top_3_last_period.append(temp, sort=False)

            historic[pk_id.split('_id')[0]] = top_3_historic.to_dict(orient='records')
            last_period[pk_id.split('_id')[0]] = top_3_last_period.to_dict(orient='records')

        result['investment_type_historic'] = historic
        result['investment_type_last_period'] = last_period

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
            Parameter(name='file_name', dtype=str)
        ]

    @staticmethod
    def steps(params):

        download_step = DownloadStep(
            connector="fdi-data-additional-3",
            connector_path="conns.yaml"
        )

        transform_step = TransformStep()

        return [download_step, transform_step]

if __name__ == "__main__":
    pp = FDIaggregatePipeline()
    for level, sheets in {'investment_type': [['7', '8', '9'], 'investment_type']}.items():
        df = pp.run({
            'level': level,
            'sheets': sheets[0],
            'file_name': sheets[1]
        })