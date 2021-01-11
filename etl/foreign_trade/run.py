import os
from google.cloud import storage
from util import get_level, check_update, LEVELS

storage_client = storage.Client.from_service_account_json(os.environ.get('GOOGLE_APPLICATION_CREDENTIALS'))
bucket = storage_client.get_bucket('datamexico-data')
blobs = bucket.list_blobs(prefix='foreign_trade')
files = [x.name for x in blobs]

mun = []
ent = []
nat = []
for file in files:
	val = ('https://storage.googleapis.com/datamexico-data/' + str(file)).split('/foreign_trade/')[1]
	if 'Municipal' in val and '.csv' in val:
		mun.append(val)
	elif 'State' in val and '.csv' in val:
		ent.append(val)
	elif 'National' in val and '.csv' in val:
		nat.append(val)

print('nat files: {}, mun files: {}, ent files: {}'.format(len(nat), len(mun), len(ent)))

for table in ['economy_foreign_trade_', 'economy_foreign_trade_unanonymized_']:
	nat = check_update(nat, table)
	for url in nat:
		type_, name_, level_name_ = get_level(url, LEVELS)
		os.system('bamboo-cli --folder . --entry foreign_trade_pipeline --url={} --type={} --name={} --table={}'.format(url, type_, name_, table))

	ent = check_update(ent, table)
	for url in ent:
		type_, name_, level_name_ = get_level(url, LEVELS)
		os.system('bamboo-cli --folder . --entry foreign_trade_pipeline --url={} --type={} --name={} --table={}'.format(url, type_, name_, table))

	mun = check_update(mun, table)
	for url in mun:
		type_, name_, level_name_ = get_level(url, LEVELS)
		os.system('bamboo-cli --folder . --entry foreign_trade_pipeline --url={} --type={} --name={} --table={}'.format(url, type_, name_, table))

"""
# countries
os.system('bamboo-cli --folder . --entry countries_ingest')

# hs6 codes
os.system('bamboo-cli --folder . --entry hs12_2digit')
os.system('bamboo-cli --folder . --entry hs12_4digit')
os.system('bamboo-cli --folder . --entry hs12_6digit')
"""