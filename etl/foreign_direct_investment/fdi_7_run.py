
from fdi_7 import FDI7Pipeline

params = [
    {'pk': 'sector_id',
     'sheet_name': '7.1',
     'level': 'sector',
     'dtype': {
         'sector_id': 'String',
         'year':      'UInt16',
         'value':     'UInt8',
         'count':     'UInt16',
         'value_c':   'Float32'
     },
     'table': 'fdi_7_sector'},
    {'pk': 'subsector_id',
     'sheet_name': '7.2',
     'level': 'subsector',
     'dtype': {
         'subsector_id': 'UInt16',
         'year':         'UInt16',
         'value':        'UInt8',
         'count':        'UInt16',
         'value_c':      'Float32'
     },
     'table': 'fdi_7_subsector'},
    {'pk': 'industry_group_id',
     'sheet_name': '7.3',
     'level': 'rama',
     'dtype': {
         'industry_group_id':  'UInt16',
         'year':               'UInt16',
         'value':              'UInt8',
         'count':              'UInt16',
         'value_c':            'Float32'
     },
     'table': 'fdi_7_industry_group'}
]

pp = FDI7Pipeline
for i in params:
    pp.run({
        'pk': i['pk'],
        'sheet_name': i['sheet_name'],
        'level': i['level'],
        'dtype': i['dtype'],
        'table': i['table'],
    })