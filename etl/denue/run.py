import os
import glob
import pandas as pd
from google.cloud import storage

storage_client = storage.Client.from_service_account_json('datamexico.json')
bucket = storage_client.get_bucket('datamexico-data')
print('Check for available folders in "datamexico-data/denue", leave black to ingest all or just "exit"')
folder = input('Enter folder: ')
blobs = bucket.list_blobs(prefix='denue/{}'.format(folder))
if folder != 'exit':
    for blob in blobs:
        if 'index' not in blob.name:
            date = blob.name.split('denue/')[1].split('/')[0].replace('_', '-')
            date = (date.split('-')[2] + '-' + date.split('-')[1] + '-' + date.split('-')[0]).replace('-', '')
            # runs pipeline
            os.system("bamboo-cli --folder . --entry companies_pipeline --date=" + date + " --url=" + str(blob.name))

    # create dimension table
    os.system('bamboo-cli --folder . --entry companies_index_ingest')
else:
    pass