
import unicodedata
import pandas as pd
from bamboo_lib.helpers import query_to_df
from bamboo_lib.models import PipelineStep
from bamboo_lib.connectors.models import Connector

COLUMNS = {
    'semana': 'approved_week',
    'entidad': 'ent_id',
    'municipio': 'mun_id',
    'genero': 'sex',
    'tipo_persona': 'person_type',
    'tamanio_patron': 'company_size',
    'rango_edad_antigüedad': 'age_range',
    'conteo_anonimizado': 'count'
}

AGE_RANGE = {
    '0-5 años': 1, 
    '5 a 9 años': 2, 
    '10 a 19 años': 3, 

    '19 años o menos': 4,

    '20 a 29 años': 5, 
    '30 a 39 años': 6, 
    '40 a 49 años': 7, 
    '50 a 59 años': 8,
    '60 y más años': 9, 
    '60 años o más': 9, 

    '20 a 49 años': 12,
    '50 a 69 años': 13, 
    '70 y más años': 14
}

COMPANY_SIZE = {
    'De 1 a 10': 1, 
    'De 11 a 20': 2, 
    'De 21 a 50': 3, 
    'Mas de 50': 4
}

PERSON_TYPE = {
    'fisica': 1,
    'moral': 2
}

SEX = {
    'H': 1,
    'M': 2,
    'NO_IDENTIFICADO': 0
}

MISSING_MUN = {
    'SILAO': 'SILAO DE LA VICTORIA',
    'SAN PEDRO MIXTEPEC -DTO. 22 -': 20318,
    'MEDELLIN': 'MEDELLIN DE BRAVO',
    'HEROICA CIUDAD DE JUCHITAN DE ZARAGOZA': 'JUCHITAN DE ZARAGOZA',
    'GRAL. ESCOBEDO': 19021,
    'TLAQUEPAQUE': 'SAN PEDRO TLAQUEPAQUE',
    'TLALTIZAPAN': 'TLALTIZAPAN DE ZAPATA'
}

APPROVED_WEEK = {
    'Del 30 de abril al 6 de mayo': 202018,
    'Del 7 al 13 de mayo': 202019,
    'Del 14 al 20 de mayo': 202020,
    'Del 21 al 27 de mayo': 202021,
    'Del 28 de mayo al 3 de junio': 202022,
    'Del 4 al 10 de junio': 202023,
    'Del 11 al 17 de junio': 202024,
    'Del 18 al 24 de junio': 202025,
    'Del 25 de junio al 1 de julio': 202026,
    'Del 2 al 8 de julio': 202027,
    'Del 9 al 15 de julio': 202028,
    'Del 16 al 22 de julio': 202029,
    'Del 23 al 29 de julio': 202030,
    'Del 30 de julio al 5 de agosto': 202031,
    'Del 6 al 12 de agosto': 202032,
    'Del 13 al 19 de agosto': 202033,
    'Del 20 al 26 de agosto': 202034,
    'Del 27 al 31 de agosto': 202035
}


def norm(string):
    return unicodedata.normalize('NFKD', string).encode('ASCII', 'ignore').decode("latin-1")

def replace_geo():
    """ query for ent, mun geo dimension"""
    db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))

    ent = query_to_df(db_connector, 'select ent_id, ent_name from dim_shared_geography_ent')
    ent['ent_name'] = ent['ent_name'].apply(lambda x: norm(x)).str.upper()
    ent = dict(zip(ent['ent_name'], ent['ent_id']))

    mun = query_to_df(db_connector, 'select mun_id, mun_name from dim_shared_geography_mun')
    mun['mun_name'] = mun['mun_name'].apply(lambda x: norm(x)).str.upper()
    mun = dict(zip(mun['mun_name'], mun['mun_id']))

    return [ent, mun] 

class ReadStep(PipelineStep):
    def run_step(self, prev, params):
        df = pd.read_csv(prev[0], encoding='latin-1')
        df.columns = df.columns.str.lower()
        df.rename(columns=COLUMNS, inplace=True)
        df['mun_id'] = '0'
        df['level'] = 'State'
        df_ent = df.copy()

        df = pd.read_csv(prev[1], encoding='latin-1')
        df.columns = df.columns.str.lower()
        df.rename(columns=COLUMNS, inplace=True)
        df['level'] = 'Municipality'

        df = df.append(df_ent, sort=False)

        return df