import os
from google.cloud import storage
from util import get_level

storage_client = storage.Client.from_service_account_json('datamexico.json')
bucket = storage_client.get_bucket('datamexico-data')
blobs = bucket.list_blobs(prefix='foreign_trade')

mun = []
ent = []
nat = []
for blob in blobs:
  val = 'https://storage.googleapis.com/datamexico-data/' + str(blob.name)
  if 'Municipal' in val and '.csv' in val:
      mun.append(val)
  elif 'State' in val and '.csv' in val:
      ent.append(val)
  elif 'National' in val and '.csv' in val:
      nat.append(val)

levels = {'National':  ['UInt8',  'ent'], 
          'State':     ['UInt8',  'ent'], 
          'Municipal': ['UInt16', 'mun']}

"""for url in nat:
  type_, name_ = get_level(url, levels)
  os.system('bamboo-cli --folder . --entry foreign_trade_pipeline --url={} --type={} --name={}'.format(url, type_, name_))"""
for url in ent:
  type_, name_ = get_level(url, levels)
  os.system('bamboo-cli --folder . --entry foreign_trade_pipeline --url={} --type={} --name={}'.format(url, type_, name_))
for url in mun:
  type_, name_ = get_level(url, levels)
  os.system('bamboo-cli --folder . --entry foreign_trade_pipeline --url={} --type={} --name={}'.format(url, type_, name_))
"""
# countries
os.system('bamboo-cli --folder . --entry countries_ingest')

# hs6 codes
os.system('bamboo-cli --folder . --entry hs12_2digit')
os.system('bamboo-cli --folder . --entry hs12_4digit')
os.system('bamboo-cli --folder . --entry hs12_6digit')"""