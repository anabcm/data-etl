
from fdi_8 import FDI8Pipeline

params = [
    {'pk': 'sector_id',
     'sheet_name': '8.1',
     'level': 'sector',
     'columns': [
         'sector_id', 'year', 'quarter_id', 'value_between_companies', 'value_new_investments',
         'value_re_investments', 'count_between_companies', 'count_new_investments', 'count_re_investments',
         'value_between_companies_c', 'value_new_investments_c', 'value_re_investments_c'
     ],
     'dtype': {
         'sector_id':        'String',
         'quarter_id':       'UInt16',
         'investment_type':  'UInt8',
         'count':            'UInt16',
         'value_c':          'Float32'
     },
     'table': 'fdi_8_sector_investment'},
    {'pk': 'subsector_id',
     'sheet_name': '8.2',
     'level': 'subsector',
     'columns': [
         'subsector_id', 'year', 'quarter_id', 'value_between_companies', 'value_new_investments',
         'value_re_investments', 'count_between_companies', 'count_new_investments', 'count_re_investments',
         'value_between_companies_c', 'value_new_investments_c', 'value_re_investments_c'
     ],
     'dtype': {
         'subsector_id':     'UInt16',
         'quarter_id':       'UInt16',
         'investment_type':  'UInt8',
         'count':            'UInt16',
         'value_c':          'Float32'
     },
     'table': 'fdi_8_subsector_investment'},
    {'pk': 'industry_group_id',
     'sheet_name': '8.3',
     'level': 'rama',
     'columns': [
         'industry_group_id', 'year', 'quarter_id', 'value_between_companies', 'value_new_investments',
         'value_re_investments', 'count_between_companies', 'count_new_investments', 'count_re_investments',
         'value_between_companies_c', 'value_new_investments_c', 'value_re_investments_c'
     ],
     'dtype': {
         'industry_group_id':  'UInt16',
         'quarter_id':         'UInt16',
         'investment_type':    'UInt8',
         'count':              'UInt16',
         'value_c':            'Float32'
     },
     'table': 'fdi_8_industry_group_investment'}
]

pp = FDI8Pipeline
for i in params:
    pp.run({
        'pk': i['pk'],
        'sheet_name': i['sheet_name'],
        'level': i['level'],
        'dtype': i['dtype'],
        'columns': i['columns'],
        'table': i['table'],
    })