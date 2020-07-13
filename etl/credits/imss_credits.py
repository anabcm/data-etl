
import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import Parameter, EasyPipeline, PipelineStep
from bamboo_lib.steps import DownloadStep, LoadStep
from shared import COLUMNS, AGE_RANGE, COMPANY_SIZE, PERSON_TYPE, SEX, replace_geo, norm

class TransformStep(PipelineStep):
    def run_step(self, prev, params):

        #url = 'data/imss_total_estatal.csv'
        df = pd.read_csv(prev[0], encoding='latin-1')
        df.columns = df.columns.str.lower()
        df.rename(columns=COLUMNS, inplace=True)
        df['mun_id'] = '0'
        df['level'] = 'State'
        df_ent = df.copy()

        #url = 'data/imss_total_municipal.csv'
        df = pd.read_csv(prev[1], encoding='latin-1')
        df.columns = df.columns.str.lower()
        df.rename(columns=COLUMNS, inplace=True)
        df['level'] = 'Municipality'

        df = df.append(df_ent, sort=False)

        # filter confidential values
        df = df.loc[df['count'] != 'C'].copy()

        # replace members in dimensions
        df['sex'].replace(SEX, inplace=True)
        df['person_type'].replace(PERSON_TYPE, inplace=True)
        df['company_size'].replace(COMPANY_SIZE, inplace=True)
        df['age_range'].replace(AGE_RANGE, inplace=True)

        for col in ['ent_id', 'mun_id']:
            df[col] = df[col].apply(lambda x: norm(x)).str.upper()

        # replace missing municipalities
        missing_municipalities = {
            'SILAO': 'SILAO DE LA VICTORIA',
            'SAN PEDRO MIXTEPEC -DTO. 22 -': 20318,
            'MEDELLIN': 'MEDELLIN DE BRAVO',
            'HEROICA CIUDAD DE JUCHITAN DE ZARAGOZA': 'JUCHITAN DE ZARAGOZA',
            'GRAL. ESCOBEDO': 19021,
            'TLAQUEPAQUE': 'SAN PEDRO TLAQUEPAQUE',
            'TLALTIZAPAN': 'TLALTIZAPAN DE ZAPATA'
        }
        df['mun_id'].replace(missing_municipalities, inplace=True)

        # replace ent
        df['ent_id'].replace({'MEXICO': 15}, inplace=True)

        # replace names for ids
        ent, mun = replace_geo()
        df['ent_id'] = df['ent_id'].replace(ent)
        df['mun_id'] = df['mun_id'].replace(mun)

        df.loc[~df['mun_id'].isin(list(mun.values())), 'mun_id'] = \
            df.loc[~df['mun_id'].isin(list(mun.values())), 'ent_id'].astype(str) + '999'
        df.loc[df['level'] == 'Municipality', 'ent_id'] = '0'

        df = df[['ent_id', 'mun_id', 'level', 'sex', 'person_type', 'company_size', 'age_range', 'count']].copy()

        for col in df.columns[df.columns != 'level']:
            df[col] = df[col].astype(int)

        return df

class IMSSCreditsPipeline(EasyPipeline):
    @staticmethod
    def steps(params):
        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))

        dtype = {
            'ent_id':       'UInt8', 
            'mun_id':       'UInt16', 
            'level':        'String', 
            'sex':          'UInt8', 
            'person_type':  'UInt8', 
            'company_size': 'UInt8', 
            'age_range':    'UInt8', 
            'count':        'UInt32'
        }

        download_step = DownloadStep(
            connector=['imss-ent-total', 'imss-mun-total'],
            connector_path='conns.yaml'
        )

        transform_step = TransformStep()

        load_step = LoadStep(
            'imss_credits', db_connector, dtype=dtype, if_exists='drop',
            pk=['ent_id', 'mun_id', 'level']
        )

        return [download_step, transform_step, load_step]

if __name__ == "__main__":
    pp = IMSSCreditsPipeline()
    pp.run({})