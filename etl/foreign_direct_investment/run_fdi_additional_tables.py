
from fdi_additional_tables import Pipeline

params = [
    {'pk': 'sector_id',
     'sheet_name': '1',
     'dtype': {
         'sector_id':         'String',
         'subsector_id':      'UInt16',
         'industry_group_id': 'UInt16',
         'quarter_id':        'UInt16',
         'investment_type':   'UInt8',
         'value':             'Float32',
         'count':             'UInt16'
     },
     'if_exists': 'append',
     'db-source': 'fdi-data-additional-2',
     'table': 'fdi_quarter_industry_investment'},
    {'pk': 'subsector_id',
     'sheet_name': '2',
     'dtype': {
         'sector_id':         'String',
         'subsector_id':      'UInt16',
         'industry_group_id': 'UInt16',
         'quarter_id':        'UInt16',
         'investment_type':   'UInt8',
         'value':             'Float32',
         'count':             'UInt16'
     },
     'if_exists': 'append',
     'db-source': 'fdi-data-additional-2',
     'table': 'fdi_quarter_industry_investment'},
    {'pk': 'industry_group_id',
     'sheet_name': '3',
     'dtype': {
         'sector_id':         'String',
         'subsector_id':       'UInt16',
         'industry_group_id':  'UInt16',
         'quarter_id':         'UInt16',
         'investment_type':    'UInt8',
         'value':              'Float32',
         'count':              'UInt16'
     },
     'if_exists': 'append',
     'db-source': 'fdi-data-additional-2',
     'table': 'fdi_quarter_industry_investment'},
    {'pk': 'sector_id',
     'sheet_name': '4',
     'dtype': {
         'sector_id':         'String',
         'subsector_id':       'UInt16',
         'industry_group_id':  'UInt16',
         'year':               'UInt16',
         'country_id':         'String',
         'value':              'Float32',
         'count':              'UInt16'
     },
     'if_exists': 'append',
     'db-source': 'fdi-data-additional',
     'table': 'fdi_year_industry_country'},
    {'pk': 'subsector_id',
     'sheet_name': '5',
     'dtype': {
         'sector_id':         'String',
         'subsector_id':       'UInt16',
         'industry_group_id':  'UInt16',
         'year':               'UInt16',
         'country_id':         'String',
         'value':              'Float32',
         'count':              'UInt16'
     },
     'if_exists': 'append',
     'db-source': 'fdi-data-additional',
     'table': 'fdi_year_industry_country'},
    {'pk': 'industry_group_id',
     'sheet_name': '6',
     'dtype': {
         'sector_id':         'String',
         'subsector_id':      'UInt16',
         'industry_group_id': 'UInt16',
         'year':              'UInt16',
         'country_id':        'String',
         'value':             'Float32',
         'count':             'UInt16'
     },
     'if_exists': 'append',
     'db-source': 'fdi-data-additional',
     'table': 'fdi_year_industry_country'},
    {'pk': 'sector_id',
     'sheet_name': '7',
     'dtype': {
         'sector_id':         'String',
         'subsector_id':      'UInt16',
         'industry_group_id': 'UInt16',
         'ent_id':            'UInt8',
         'year':              'UInt16',
         'value':             'Float32',
         'value_c':           'Float32',
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
         'value_c':           'Float32',
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
         'value_c':           'Float32',
         'count':             'UInt16'
     },
     'if_exists': 'append',
     'db-source': 'fdi-data-additional',
     'table': 'fdi_year_state_industry'},
    {'pk': 'sector_id',
     'sheet_name': '1',
     'dtype': {
         'sector_id':         'String',
         'subsector_id':      'UInt16',
         'industry_group_id': 'UInt16',
         'year':              'UInt16',
         'value':             'Float32',
         'value_c':           'Float32',
         'count':             'UInt16'
     },
     'if_exists': 'append',
     'db-source': 'fdi-data-additional-3',
     'table': 'fdi_year_industry'},
    {'pk': 'sector_id',
     'sheet_name': '2',
     'dtype': {
         'sector_id':         'String',
         'subsector_id':      'UInt16',
         'industry_group_id': 'UInt16',
         'year':              'UInt16',
         'value':             'Float32',
         'value_c':           'Float32',
         'count':             'UInt16'
     },
     'if_exists': 'append',
     'db-source': 'fdi-data-additional-3',
     'table': 'fdi_year_industry'},
    {'pk': 'industry_group_id',
     'sheet_name': '3',
     'dtype': {
         'sector_id':         'String',
         'subsector_id':      'UInt16',
         'industry_group_id': 'UInt16',
         'year':              'UInt16',
         'value':             'Float32',
         'value_c':           'Float32',
         'count':             'UInt16'
     },
     'if_exists': 'append',
     'db-source': 'fdi-data-additional-3',
     'table': 'fdi_year_industry'},
    {'pk': 'sector_id',
     'sheet_name': '4',
     'dtype': {
         'sector_id':         'String',
         'subsector_id':      'UInt16',
         'industry_group_id': 'UInt16',
         'quarter_id':        'UInt16',
         'value':             'Float32',
         'value_c':           'Float32',
         'count':             'UInt16'
     },
     'if_exists': 'append',
     'db-source': 'fdi-data-additional-3',
     'table': 'fdi_quarter_industry'},
    {'pk': 'subsector_id',
     'sheet_name': '5',
     'dtype': {
         'sector_id':         'String',
         'subsector_id':      'UInt16',
         'industry_group_id': 'UInt16',
         'quarter_id':        'UInt16',
         'value':             'Float32',
         'value_c':           'Float32',
         'count':             'UInt16'
     },
     'if_exists': 'append',
     'db-source': 'fdi-data-additional-3',
     'table': 'fdi_quarter_industry'},
    {'pk': 'industry_group_id',
     'sheet_name': '6',
     'dtype': {
         'sector_id':         'String',
         'subsector_id':      'UInt16',
         'industry_group_id': 'UInt16',
         'quarter_id':        'UInt16',
         'value':             'Float32',
         'value_c':           'Float32',
         'count':             'UInt16'
     },
     'if_exists': 'append',
     'db-source': 'fdi-data-additional-3',
     'table': 'fdi_quarter_industry'},
    {'pk': 'sector_id',
     'sheet_name': '7',
     'dtype': {
         'sector_id':         'String',
         'subsector_id':      'UInt16',
         'industry_group_id': 'UInt16',
         'investment_type':   'UInt8',
         'year':              'UInt16',
         'value':             'Float32',
         'value_c':           'Float32',
         'count':             'UInt16'
     },
     'if_exists': 'append',
     'db-source': 'fdi-data-additional-3',
     'table': 'fdi_year_investment_industry'},
    {'pk': 'subsector_id',
     'sheet_name': '8',
     'dtype': {
         'sector_id':         'String',
         'subsector_id':      'UInt16',
         'industry_group_id': 'UInt16',
         'investment_type':   'UInt8',
         'year':              'UInt16',
         'value':             'Float32',
         'value_c':           'Float32',
         'count':             'UInt16'
     },
     'if_exists': 'append',
     'db-source': 'fdi-data-additional-3',
     'table': 'fdi_year_investment_industry'},
    {'pk': 'industry_group_id',
     'sheet_name': '9',
     'dtype': {
         'sector_id':         'String',
         'subsector_id':      'UInt16',
         'industry_group_id': 'UInt16',
         'investment_type':   'UInt8',
         'year':              'UInt16',
         'value':             'Float32',
         'value_c':           'Float32',
         'count':             'UInt16'
     },
     'if_exists': 'append',
     'db-source': 'fdi-data-additional-3',
     'table': 'fdi_year_investment_industry'}
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