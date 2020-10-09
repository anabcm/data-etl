
from fdi_additional_tables import Pipeline

params = [
    {'pk': 'sector_id',
     'sheet_name': '1',
     'dtype': {
         'sector_id':       'String',
         'quarter_id':      'UInt16',
         'investment_type': 'UInt8',
         'value':           'Float32',
         'count':           'UInt16'
     },
     'if_exists': 'drop',
     'db-source': 'fdi-data-additional-2',
     'table': 'fdi_quarter_industry_sector'},
    {'pk': 'subsector_id',
     'sheet_name': '2',
     'dtype': {
         'subsector_id':    'UInt16',
         'quarter_id':      'UInt16',
         'investment_type': 'UInt8',
         'value':           'Float32',
         'count':           'UInt16'
     },
     'if_exists': 'drop',
     'db-source': 'fdi-data-additional-2',
     'table': 'fdi_quarter_industry_subsector'},
    {'pk': 'industry_group_id',
     'sheet_name': '3',
     'dtype': {
         'industry_group_id':  'UInt16',
         'quarter_id':         'UInt16',
         'investment_type':    'UInt8',
         'value':              'Float32',
         'count':              'UInt16'
     },
     'if_exists': 'drop',
     'db-source': 'fdi-data-additional-2',
     'table': 'fdi_quarter_industry_industry_group'},
    {'pk': 'sector_id',
     'sheet_name': '4',
     'dtype': {
         'sector_id':  'String',
         'year':       'UInt16',
         'country_id': 'String',
         'value':      'Float32',
         'count':      'UInt16'
     },
     'if_exists': 'drop',
     'db-source': 'fdi-data-additional',
     'table': 'fdi_year_sector_country'},
    {'pk': 'subsector_id',
     'sheet_name': '5',
     'dtype': {
         'subsector_id': 'UInt16',
         'year':         'UInt16',
         'country_id':   'String',
         'value':        'Float32',
         'count':        'UInt16'
     },
     'if_exists': 'drop',
     'db-source': 'fdi-data-additional',
     'table': 'fdi_year_subsector_country'},
    {'pk': 'industry_group_id',
     'sheet_name': '6',
     'dtype': {
         'industry_group_id': 'UInt16',
         'year':              'UInt16',
         'country_id':        'String',
         'value':             'Float32',
         'count':             'UInt16'
     },
     'if_exists': 'drop',
     'db-source': 'fdi-data-additional',
     'table': 'fdi_year_industry_group_country'},
    {'pk': 'sector_id',
     'sheet_name': '7',
     'dtype': {
         'sector_id':         'String',
         'subsector_id':      'UInt16',
         'industry_group_id': 'UInt16',
         'ent_id':            'UInt8',
         'year':              'UInt16',
         'value':             'Float32',
         'value_C':           'Float32',
         'count':             'UInt16'
     },
     'if_exists': 'append',
     'db-source': 'fdi-data-additional',
     'table': 'fdi_year_state_industry'},
    {'pk': 'subsector_id',
     'sheet_name': '8',
     'dtype': {
         'sector_id':         'String',
         'subsector_id':      'UInt16',
         'industry_group_id': 'UInt16',
         'ent_id':            'UInt8',
         'year':              'UInt16',
         'value':             'Float32',
         'value_C':           'Float32',
         'count':             'UInt16'
     },
     'if_exists': 'append',
     'db-source': 'fdi-data-additional',
     'table': 'fdi_year_state_industry'},
    {'pk': 'industry_group_id',
     'sheet_name': '9',
     'dtype': {
         'sector_id':         'String',
         'subsector_id':      'UInt16',
         'industry_group_id': 'UInt16',
         'ent_id':            'UInt8',
         'year':              'UInt16',
         'value':             'Float32',
         'value_C':           'Float32',
         'count':             'UInt16'
     },
     'if_exists': 'append',
     'db-source': 'fdi-data-additional',
     'table': 'fdi_year_state_industry'}
]

pp = Pipeline
for i in params:
    pp.run({
        'pk': i['pk'],
        'sheet_name': i['sheet_name'],
        'dtype': i['dtype'],
        'table': i['table'],
        'db-source': i['db-source'],
        'if_exists': i['if_exists']
    })