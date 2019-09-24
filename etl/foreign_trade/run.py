import os

params = {
    'level': ['State', 'Municipal'],
    'period': ['Annual', 'Monthly'],
    'depth': {'2D': 2,
              '4D': 4,
              '6D': 6},
    'years': range(6, 20),
    'months': range(1, 13),
}

# months
period = params['period'][1]
for level in params['level']:
  for k,v in params['depth'].items():
    for year in params['years']:
      for month in params['months']:
        if level == 'State':
          command = ('bamboo-cli --folder . --entry foreign_trade_pipeline --level={} --depth_name={} --depth_value={} --period={} --year={} --month={} --column_name={} --type={}').format(level, k, v, period, str(year).zfill(2), str(month).zfill(2), 'ent', 'UInt8')
          os.system(command)
        elif level == 'Municipal':
          command = ('bamboo-cli --folder . --entry foreign_trade_pipeline --level={} --depth_name={} --depth_value={} --period={} --year={} --month={} --column_name={} --type={}').format(level, k, v, period, str(year).zfill(2), str(month).zfill(2), 'mun', 'UInt16')
          os.system(command)
          

# annual
period = params['period'][0]
for level in params['level']:
  for k,v in params['depth'].items():
    for year in params['years']:
      if k == '2D' and level == 'State':
        continue
      elif level == 'State':
        command = ('bamboo-cli --folder . --entry foreign_trade_pipeline --level={} --depth_name={} --depth_value={} --period={} --year={} --column_name={} --type={}').format(level, k, v, period, str(year).zfill(2), 'ent', 'UInt8')
        os.system(command)
      elif level == 'Municipal':
        command = ('bamboo-cli --folder . --entry foreign_trade_pipeline --level={} --depth_name={} --depth_value={} --period={} --year={} --column_name={} --type={}').format(level, k, v, period, str(year).zfill(2), 'mun', 'UInt16')
        os.system(command)

# countries
#os.system('bamboo-cli --folder . --entry countries_ingest')

# hs6 2012
os.system('bamboo-cli --folder . --entry hs_codes_ingest')