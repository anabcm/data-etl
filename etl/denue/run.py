import os
import glob
import pandas as pd

from google.cloud import storage

storage_client = storage.Client.from_service_account_json('datamexico.json')
bucket = storage_client.get_bucket('datamexico-data')
folder = input('Enter folder: ')
blobs = bucket.list_blobs(prefix='denue/{}'.format(folder))
urls = []
years = []
for blob in blobs:
    if 'index' not in blob.name:
        url = 'https://storage.googleapis.com/datamexico-data/' + str(blob.name)
        date = blob.name.split('denue/')[1].split('/')[0].replace('_', '-')
        date = (date.split('-')[2] + '-' + date.split('-')[1] + '-' + date.split('-')[0]).replace('-', '')
        # runs pipeline
        os.system("bamboo-cli --folder . --entry companies_pipeline --date=" + date + " --url=" + url)

# create dimension table
os.system('bamboo-cli --folder . --entry companies_index_ingest')