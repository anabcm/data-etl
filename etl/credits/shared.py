
import unicodedata
from bamboo_lib.helpers import query_to_df
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
    '20 a 29 años': 4, 
    '30 a 39 años': 5, 
    '40 a 49 años': 6, 
    '50 a 59 años': 7,
    '60 y más años': 8, 
    
    '19 años o menos': 11,
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


def norm(string):
    return unicodedata.normalize('NFKD', string).encode('ASCII', 'ignore').decode("latin-1")

def replace_geo():
    db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))
    ent = query_to_df(db_connector, 'select ent_id, ent_name from dim_shared_geography_ent')
    ent['ent_name'] = ent['ent_name'].apply(lambda x: norm(x)).str.upper()
    ent = dict(zip(ent['ent_name'], ent['ent_id']))

    db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))
    mun = query_to_df(db_connector, 'select mun_id, mun_name from dim_shared_geography_mun')
    mun['mun_name'] = mun['mun_name'].apply(lambda x: norm(x)).str.upper()
    mun = dict(zip(mun['mun_name'], mun['mun_id']))

    return [ent, mun] 