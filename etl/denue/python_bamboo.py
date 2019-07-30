#!/usr/bin/env python
# coding: utf-8

import os
import glob
import pandas as pd

from google.cloud import storage

storage_client = storage.Client.from_service_account_json('datamexico.json')
bucket = storage_client.get_bucket('datamexico-data')
blobs = bucket.list_blobs()
urls = []
years = []
for blob in blobs:
    if 'denue' in blob.name:
        #urls.append('https://storage.googleapis.com/datamexico-data/' + str(blob.name))
        url = 'https://storage.googleapis.com/datamexico-data/' + str(blob.name)
        date = blob.name.split('denue/')[1].split('/')[0].replace('_', '-')
        date = (date.split('-')[2] + '-' + date.split('-')[1] + '-' + date.split('-')[0]).replace('-', '')
        os.system("bamboo-cli --folder . --entry companies_pipeline --date=" + date + " --url=" + url)