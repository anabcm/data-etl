
import json
import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import Parameter, EasyPipeline, PipelineStep, Parameter
from bamboo_lib.steps import DownloadStep
from shared import get_dimensions, LIMIT_C, INVESTMENT_TYPE

COLUMNS = {
    'Entidad federativa': 'ent_id',
    'Año': 'year',
    'Trimestre': 'quarter_id',
    'Monto cuentas entre compañías': 'value_between_companies',
    'Monto nuevas inversiones': 'value_new_investments',
    'Monto reinversión de utilidades': 'value_re_investments',
    'Recuento cuentas entre compañías': 'count_between_companies',
    'Recuento nuevas inversiones': 'count_new_investments',
    'Recuento reinversión de utilidades': 'count_re_investments',
    'Monto C cuentas entre compañías': 'value_between_companies_c',
    'Monto C nuevas inversiones': 'value_new_investments_c',
    'Monto C reinversión de utilidades': 'value_re_investments_c'
}

class TransformStep(PipelineStep):
    def run_step(self, prev, params):

        df = pd.read_excel(prev, sheet_name='2.4')

        pk_id = 'ent_id'

        df.columns = [x.strip() for x in df.columns]

        df.rename(columns=COLUMNS, inplace=True)

        df = df[list(COLUMNS.values())]

        df = df.loc[~df['ent_id'].str.contains('Total')].copy()

        # get end_id dimension
        dim_geo = get_dimensions()[0]

        df['ent_name'] = df['ent_id']
        df['ent_id'].replace(dict(zip(dim_geo['ent_name'], dim_geo['ent_id'])), inplace=True)

        df['year'] = df['year'].astype(int)

        base = ['ent_id', 'ent_name', 'year', 'quarter_id']
        df_final = pd.DataFrame()
        for option in ['between_companies', 'new_investments', 're_investments']:
            temp = df[base + ['count_{}'.format(option), 'value_{}_c'.format(option), 'value_{}'.format(option)]].copy()
            temp.columns = ['ent_id', 'ent_name', 'year', 'quarter_id', 'count', 'value_c', 'value']
            temp.dropna(subset=['value_c'], inplace=True)
            temp['investment_type'] = option
            df_final = df_final.append(temp)
        df = df_final.copy()

        df['investment_type'].replace(INVESTMENT_TYPE, inplace=True)
        df['investment_type'].replace({
            1: 'Cuentas entre compañías',
            2: 'Nuevas inversiones',
            3: 'Reinversión de utilidades'
        }, inplace=True)

        result = {}
        historic = {}
        last_period = {}

        temp = pd.DataFrame()
        top_3_historic = pd.DataFrame()
        top_3_last_period = pd.DataFrame()

        for ele in list(df[pk_id].unique()):
            # IED 1999 - 2020
            temp = df.loc[df[pk_id] == ele, [params.get('level'), pk_id, 'ent_name', 'value', 'count', 'value_c']] \
                .groupby(by=[params.get('level'), pk_id, 'ent_name']).sum().reset_index().sort_values(by=['value'], ascending=False)

            # "C" values
            temp.loc[(temp['count'] < LIMIT_C) & (temp['count'] != 0), 'value'] = 'C'

            top_3_historic = top_3_historic.append(temp, sort=False)

            # IED ultimo periodo
            temp = df.loc[df[pk_id] == ele, ['year', params.get('level'), pk_id, 'ent_name', 'value', 'count']] \
                .groupby(by=['year', params.get('level'), pk_id, 'ent_name']).sum().reset_index()
            temp = temp.loc[temp['year'] == temp['year'].max()].sort_values(by=['value'], ascending=False)

            # "C" values
            temp.loc[(temp['count'] < LIMIT_C) & (temp['count'] != 0), 'value'] = 'C'

            top_3_last_period = top_3_last_period.append(temp, sort=False)

        result['geo_investment_type_historic'] = top_3_historic.to_dict(orient='records')
        result['geo_investment_type_last_period'] = top_3_last_period.to_dict(orient='records')

        with open('{}.json'.format(params.get('file_name')), 'w') as outfile:
            json.dump(result, outfile)

        return df


class FDIaggregatePipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return [
            Parameter(name='level', dtype=str),
            Parameter(name='file_name', dtype=str)
        ]

    @staticmethod
    def steps(params):

        download_step = DownloadStep(
            connector="fdi-data",
            connector_path="conns.yaml",
            force=True
        )

        transform_step = TransformStep()

        return [download_step, transform_step]

if __name__ == "__main__":
    pp = FDIaggregatePipeline()
    for level, file_name in {'investment_type': 'geo_investment_type'}.items():
        df = pp.run({
            'level': level,
            'file_name': file_name
        })