import os

params = {
    'level': {'National':  ['nat', 'ent', 'UInt8'],
              'State':     ['state', 'ent', 'UInt8'], 
              'Municipal': ['mun', 'mun', 'UInt16']},
    'period': ['Annual', 'Monthly'],
    'depth': {'2D': 2,
              '4D': 4,
              '6D': 6},
    'years': range(6, 20),
    'months': range(1, 13),
}

# months
period = params['period'][1]
for level, values in params['level'].items():
  for k,v in params['depth'].items():
    for year in params['years']:
      for month in params['months']:
        if level == 'National' and k == '2D':
          continue
        else:
          command = ('bamboo-cli --folder . --entry foreign_trade_pipeline --level={} --prefix={} --depth_name={} --depth_value={} --period={} --year={} --month={} --column_name={} --type={}').format(level, values[0], k, v, period, str(year).zfill(2), str(month).zfill(2), values[1], values[2])
          os.system(command)

# annual
period = params['period'][0]
for level, values in params['level'].items():
  for k,v in params['depth'].items():
    for year in params['years']:
      if k != '2D':
        command = ('bamboo-cli --folder . --entry foreign_trade_pipeline --level={} --prefix={} --depth_name={} --depth_value={} --period={} --year={} --column_name={} --type={}').format(level, values[0], k, v, period, str(year).zfill(2), values[1], values[2])
        os.system(command)

# countries
#os.system('bamboo-cli --folder . --entry countries_ingest')

# hs6 codes
os.system('bamboo-cli --folder . --entry hs12_2digit')
os.system('bamboo-cli --folder . --entry hs12_4digit')
os.system('bamboo-cli --folder . --entry hs12_6digit')