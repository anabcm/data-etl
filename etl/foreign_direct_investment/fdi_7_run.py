
from fdi_7 import FDI7Pipeline

params = [
    {'pk': 'sector_id',
     'sheet_name': '7.1',
     'level': 'sector',
     'dtype': {
         'sector_id': 'UInt8',
         'year':      'UInt16',
         'value':     'Float32',
         'count':     'UInt16',
         'value_c':   'UInt8'
     },
     'table': 'fdi_7_sector'},
    {'pk': 'subsector_id',
     'sheet_name': '7.2',
     'level': 'subsector',
     'dtype': {
         'subsector_id': 'UInt16',
         'year':         'UInt16',
         'value':        'Float32',
         'count':        'UInt16',
         'value_c':      'UInt8'
     },
     'table': 'fdi_7_subsector'},
    {'pk': 'rama_id',
     'sheet_name': '7.3',
     'level': 'rama',
     'dtype': {
         'rama_id':   'UInt16',
         'year':      'UInt16',
         'value':     'Float32',
         'count':     'UInt16',
         'value_c':   'UInt8'
     },
     'table': 'fdi_7_rama'}
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