import os

for year in range(6, 19):
  for month in range(1, 13):
    try:
      url = 'https://storage.googleapis.com/datamexico-data/foreign_trade/Municipal/HS_6D/Monthly/6D_mun_month' + str(month).zfill(2) + str(year).zfill(2) + '.csv'
      os.system('bamboo-cli --folder . --entry foreign_trade_pipeline --url=' + url)
    except:
      url = 'https://storage.googleapis.com/datamexico-data/foreign_trade/Municipal/HS_6D/Monthly/6D_mun_monthly' + str(month).zfill(2) + str(year).zfill(2) + '.csv'
      os.system('bamboo-cli --folder . --entry foreign_trade_pipeline --url=' + url)

# campus index
os.system('bamboo-cli --folder . --entry countries_ingest')