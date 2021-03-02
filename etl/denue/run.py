import os
import glob
import pandas as pd
from google.cloud import storage

storage_client = storage.Client.from_service_account_json(os.environ.get('GOOGLE_APPLICATION_CREDENTIALS'))
bucket = storage_client.get_bucket('datamexico-data')
print('Check for available folders in "datamexico-data/denue", leave blank to ingest all or just "exit"')
folder = input('Enter folder: ')
blobs = bucket.list_blobs(prefix='denue/{}'.format(folder))
routes = [x.name for x in blobs if x.name != 'denue/{}/'.format(folder)]

if folder != 'exit':
    for url in routes:
        if 'index' not in url:
            date = url.split('denue/')[1].split('/')[0].replace('_', '-')
            date = (date.split('-')[2] + '-' + date.split('-')[1] + '-' + date.split('-')[0]).replace('-', '')
            # runs pipeline
            os.system("bamboo-cli --folder . --entry companies_pipeline --date=" + date + " --url=" + url)

    # create dimension table
    # os.system('bamboo-cli --folder . --entry companies_index_ingest')
else:
    pass