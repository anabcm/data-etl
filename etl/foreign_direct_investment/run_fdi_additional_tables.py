
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
     'table': 'fdi_year_industry_sector'},
    {'pk': 'subsector_id',
     'sheet_name': '2',
     'dtype': {
         'subsector_id':    'UInt16',
         'quarter_id':      'UInt16',
         'investment_type': 'UInt8',
         'value':           'Float32',
         'count':           'UInt16'
     },
     'table': 'fdi_year_industry_subsector'},
    {'pk': 'industry_group_id',
     'sheet_name': '3',
     'dtype': {
         'industry_group_id':  'UInt16',
         'quarter_id':         'UInt16',
         'investment_type':    'UInt8',
         'value':              'Float32',
         'count':              'UInt16'
     },
     'table': 'fdi_year_industry_industry_group'}
]

pp = Pipeline
for i in params:
    pp.run({
        'pk': i['pk'],
        'sheet_name': i['sheet_name'],
        'dtype': i['dtype'],
        'table': i['table'],
    })