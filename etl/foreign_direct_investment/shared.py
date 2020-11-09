
import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.helpers import query_to_df

def get_dimensions():
    db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))
    dim_geo_query = 'SELECT ent_name, ent_id FROM dim_shared_geography_ent'
    dim_geo = query_to_df(db_connector, raw_query=dim_geo_query)

    dim_country_query = 'SELECT country_name_es, iso3 FROM dim_shared_country'
    dim_country = query_to_df(db_connector, raw_query=dim_country_query)

    return [dim_geo, dim_country]

INVESTMENT_TYPE = {
    'between_companies': 1,
    'Cuentas entre compañías': 1,
    'new_investments': 2,
    'Nuevas inversiones': 2,
    're_investments': 3,
    'Reinversión de utilidades': 3
}

SECTOR_REPLACE = {
    31: '31-33',
    32: '31-33',
    33: '31-33',
    #43: '43-46',
    #46: '43-46',
    48: '48-49',
    49: '48-49'
}

COUNTRY_REPLACE = {
    'Hong Kong (RAE de China)': 'Hong Kong',
    'Taiwán (Provincia de China)': 'Taiwan',
    'China, República Popular de': 'China',
    'China República Popular de': 'China',
    'Federación de Rusia': 'Rusia',
    'Estados Unidos de América': 'Estados Unidos',
    'Reino Unido de la Gran Bretaña e Irlanda del Norte': 'Reino Unido',
    'Corea, República de': 'Corea del Sur',
    'Corea República de': 'Corea del Sur',
    'República Checa': 'Chequia',
    'Venezuela, República Bolivariana de': 'Venezuela',
    'Venezuela República Bolivariana de': 'Venezuela',
    'Siria República Árabe de': 'syr',
    'Siria, República Árabe de': 'syr',
    'Serbia República de': 'srb',
    'Bolivia (Estado Plurinacional de)': 'bol',
    'Bermudas': 'bmu',
    'Bahamas, Las': 'bhs',
    'Irán República Islámica de': 'irn',
    'Irán, República Islámica de': 'irn',
    'Congo República Democrática del': 'cod',
    'Cisjordania y la Franja de Gaza': 'pse',
    'Serbia y Montenegro': 'scg',
    'Serbia, República de': 'srb',
    'Antillas Holandesas': 'ant',
    'Islas Vírgenes de los Estados Unidos': 'vir',
    'Suazilandia': 'swz',
    'Brunei Darussalam': 'brn',
    'Fiyi, República de': 'fji',
    'Kazajstán, República de': 'kaz',
    'Surinam': 'sur',
    'Bahréin': 'bhr',
    'Belarús': 'blr',
    'Guyana': 'guy',
    'Qatar': 'qat',
    'Yemen, República de': 'yar',
    'Micronesia, Estados Federados de': 'fsm',
    'Nepal, República Federal Democrática de': 'npl',
    'Corea, República Popular Democrática de': 'prk',
    'Guyana Francesa': 'guf',
    'Montenegro, República de': 'mne',
    'Costa de Marfil': 'civ',
    'Otros países': 'xxa'
}