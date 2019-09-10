import os

period = 'Monthly'
for depth in ['2D', '4D', '6D']:
  for year in range(6, 19):
    for month in range(1, 13):
      for url in [
        'https://storage.googleapis.com/datamexico-data/foreign_trade/Municipal/HS_' + depth + '/' + period + '/' + depth + '_mun_month' + str(month).zfill(2) + str(year).zfill(2) + '.csv',
        'https://storage.googleapis.com/datamexico-data/foreign_trade/Municipal/HS_' + depth + '/' + period + '/' + depth + '_mun_' + period.lower() + str(month).zfill(2) + str(year).zfill(2) + '.csv',
      ]:
        try:
          os.system('bamboo-cli --folder . --entry foreign_trade_pipeline --url=' + url)
        except:
          continue


period = 'Annual'
for depth in ['4D', '6D']:
    for year in range(6, 19):
      url = 'https://storage.googleapis.com/datamexico-data/foreign_trade/Municipal/HS_' + depth + '/' + period + '/' + depth + '_mun_' + period.lower() + str(year).zfill(2) + '.csv'
      try:
        os.system('bamboo-cli --folder . --entry foreign_trade_pipeline --url=' + url)
      except:
        continue

# countries
os.system('bamboo-cli --folder . --entry countries_ingest')

# hs6 2012
os.system('bamboo-cli --folder . --entry hs_codes_ingest')