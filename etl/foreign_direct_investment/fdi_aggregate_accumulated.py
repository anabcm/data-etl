
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

            if pk_id != 'country_id':
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
                temp = df.loc[df[pk_id] == ele, [pk_id, 'value', 'count', 'value_c']] \
                    .groupby(by=[pk_id]).sum().reset_index().sort_values(by=['value'], ascending=False)
                
                # 
                temp['check'] = None
                for item in temp.iterrows():
                    if item[1][2] > 2:
                        temp.loc[(temp[pk_id] == item[1][pk_id]), 'check'] = \
                            temp.loc[(temp[pk_id] == item[1][pk_id]), 'value']
                    else:
                        temp.loc[(temp[pk_id] == item[1][pk_id]), 'check'] = 'C'
                        
                top_3_historic = top_3_historic.append(temp, sort=False)

                # top 3 entidades federativas que acumulan mas IED ultimo anio
                temp = df.loc[df[pk_id] == ele, ['year', pk_id, 'value', 'count']].groupby(by=['year', pk_id]).sum().reset_index()
                temp = temp.loc[(temp[pk_id] == ele) & (temp['year'] == temp['year'].max()), ['year', pk_id, 'value', 'count']] \
                    .groupby(by=['year', pk_id]).sum().reset_index().sort_values(by=['value'], ascending=False)
                temp['check'] = None

                for item in temp.iterrows():
                    try:
                        temp.loc[(temp['year'] == item[1]['year']) & (temp[pk_id] == item[1][pk_id]), 'check'] = \
                            df.loc[(df['year'] == item[1]['year']) & (df[pk_id] == item[1][pk_id]), 'value_c'].sum()
                    except Exception as e:
                        print(e)
                        temp.loc[(temp['year'] == item[1]['year']) & (temp[pk_id] == item[1][pk_id]), 'check'] = 'C'
                top_3_last_period = top_3_last_period.append(temp, sort=False)

            if pk_id == 'country_id':
                top_3_historic.sort_values(by=['value'], ascending=False, inplace=True)
                top_3_historic.drop(columns=['value'], inplace=True)
                top_3_historic.rename(columns={'check': 'value'}, inplace=True)
                top_3_historic = top_3_historic[:3]
                top_3_last_period.sort_values(by=['value'], ascending=False, inplace=True)
                top_3_last_period.drop(columns=['value'], inplace=True)
                top_3_last_period.rename(columns={'check': 'value'}, inplace=True)
                top_3_last_period = top_3_last_period[:3]
                top_3_historic['top'] = range(1, top_3_historic.shape[0] + 1)
                top_3_last_period['top'] = range(1, top_3_last_period.shape[0] + 1)
            else:
                top_3_historic.drop(columns=['value'], inplace=True)
                top_3_historic.rename(columns={'check': 'value'}, inplace=True)
                top_3_last_period.drop(columns=['value'], inplace=True)
                top_3_last_period.rename(columns={'check': 'value'}, inplace=True)

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