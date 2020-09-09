
from fdi_8 import FDI8Pipeline

params = [
    {'pk': 'sector_id',
     'sheet_name': '8.1',
     'level': 'sector',
     'dtype': {
         'sector_id':                 'String',
         'year':                      'UInt16',
         'quarter_id':                'UInt16',
         'value_between_companies':   'Float32',
         'value_new_investments':     'Float32',
         'value_re_investments':      'Float32',
         'count_between_companies':   'UInt16',
         'count_new_investments':     'UInt16',
         'count_re_investments':      'UInt16',
         'value_between_companies_c': 'UInt8',
         'value_new_investments_c':   'UInt8',
         'value_re_investments_c':    'UInt8'
     },
     'table': 'fdi_8_sector_investment'},
    {'pk': 'subsector_id',
     'sheet_name': '8.2',
     'level': 'subsector',
     'dtype': {
         'subsector_id':              'UInt16',
         'year':                      'UInt16',
         'quarter_id':                'UInt16',
         'value_between_companies':   'Float32',
         'value_new_investments':     'Float32',
         'value_re_investments':      'Float32',
         'count_between_companies':   'UInt16',
         'count_new_investments':     'UInt16',
         'count_re_investments':      'UInt16',
         'value_between_companies_c': 'UInt8',
         'value_new_investments_c':   'UInt8',
         'value_re_investments_c':    'UInt8'
     },
     'table': 'fdi_8_subsector_investment'},
    {'pk': 'industry_group_id',
     'sheet_name': '8.3',
     'level': 'rama',
     'dtype': {
         'industry_group_id':         'UInt16',
         'year':                      'UInt16',
         'quarter_id':                'UInt16',
         'value_between_companies':   'Float32',
         'value_new_investments':     'Float32',
         'value_re_investments':      'Float32',
         'count_between_companies':   'UInt16',
         'count_new_investments':     'UInt16',
         'count_re_investments':      'UInt16',
         'value_between_companies_c': 'UInt8',
         'value_new_investments_c':   'UInt8',
         'value_re_investments_c':    'UInt8'
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
        'table': i['table'],
    })