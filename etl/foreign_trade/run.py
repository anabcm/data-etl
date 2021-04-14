import os
from google.cloud import storage
from util import get_level, check_update, LEVELS

# Creates connection to Google Cloud
storage_client = storage.Client.from_service_account_json(os.environ.get('GOOGLE_APPLICATION_CREDENTIALS'))
bucket = storage_client.get_bucket('datamexico-data')
blobs = bucket.list_blobs(prefix='foreign_trade')
# Reads all files in foreign trade folder and returns it's path (EJ: /foreign_trade/Municipal/HS_4D/Annual/4D_mun_annual06.csv)
files = [x.name for x in blobs]

mun = []
ent = []
nat = []
# For each file in the list, we append it's name to an array depending on the level of depth in it's geographical classification (Municipal, State, or National).
for file in files:
	val = ('https://storage.googleapis.com/datamexico-data/' + str(file)).split('/foreign_trade/')[1]
	if 'Municipal' in val and '.csv' in val:
		mun.append(val)
	elif 'State' in val and '.csv' in val:
		ent.append(val)
	elif 'National' in val and '.csv' in val:
		nat.append(val)

print('nat files: {}, mun files: {}, ent files: {}'.format(len(nat), len(mun), len(ent)))

# For each table (names defined for cubes), we check if there are new updates comparing with files already ingested in clickhouse database.
# This can be done by the "check_update" function, which returns just new files stored in GCP.
# We run the script "foreign_trade_pipeline" with the specific parameters.
for table in ['economy_foreign_trade_', 'economy_foreign_trade_unanonymized_']:
	nat_ = check_update(nat, table)
	for url in nat_:
		type_, name_, level_name_ = get_level(url, LEVELS)
		os.system('bamboo-cli --folder . --entry foreign_trade_pipeline --url={} --type={} --name={} --table={}'.format(url, type_, name_, table))

	ent_ = check_update(ent, table)
	for url in ent_:
		type_, name_, level_name_ = get_level(url, LEVELS)
		os.system('bamboo-cli --folder . --entry foreign_trade_pipeline --url={} --type={} --name={} --table={}'.format(url, type_, name_, table))

	mun_ = check_update(mun, table)
	for url in mun_:
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
