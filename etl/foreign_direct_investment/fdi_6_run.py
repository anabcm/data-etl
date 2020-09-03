
from fdi_6 import FDI6Pipeline

params = [
    {'pk': 'ent_id',
     'sheet_name': '6.1',
     'level': 'sector',
     'dtype': {
         'sector_id': 'String',
         'ent_id':    'UInt8',
         'value':     'Float32',
         'count':     'UInt16',
         'value_c':   'UInt8'
     },
     'table': 'fdi_6_sector'},
    {'pk': 'ent_id',
     'sheet_name': '6.2',
     'level': 'subsector',
     'dtype': {
         'subsector_id': 'UInt16',
         'ent_id':       'UInt8',
         'value':        'Float32',
         'count':        'UInt16',
         'value_c':      'UInt8'
     },
     'table': 'fdi_6_subsector'},
    {'pk': 'ent_id',
     'sheet_name': '6.3',
     'level': 'rama',
     'dtype': {
         'industry_group_id':  'UInt16',
         'ent_id':             'UInt8',
         'value':              'Float32',
         'count':              'UInt16',
         'value_c':            'UInt8'
     },
     'table': 'fdi_6_industry_group'}
]

pp = FDI6Pipeline
for i in params:
    pp.run({
        'pk': i['pk'],
        'sheet_name': i['sheet_name'],
        'level': i['level'],
        'dtype': i['dtype'],
        'table': i['table'],
    })