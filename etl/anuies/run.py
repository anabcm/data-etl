import os

data = ['https://storage.googleapis.com/datamexico-data/anuies/temp/posgrado_2017-2018.xlsx',
        'https://storage.googleapis.com/datamexico-data/anuies/temp/posgrado_2016-2017.xlsx',
        'https://storage.googleapis.com/datamexico-data/anuies/temp/licenciatura_2017-2018.xlsx',
        'https://storage.googleapis.com/datamexico-data/anuies/temp/licenciatura_2016-2017.xlsx']

cmd = ['bamboo-cli --folder . --entry enrollment_pipeline', 
       'bamboo-cli --folder . --entry origin_pipeline', 
       'bamboo-cli --folder . --entry status_pipeline']

# data
for url in data:
  for command in cmd:
    os.system(command + ' --url=' + url)

# work center index
os.system('bamboo-cli --folder . --entry universities_index_pipeline')

# careers index
os.system('bamboo-cli --folder . --entry careers_index_programs')

# campus index
os.system('bamboo-cli --folder . --entry campus_index_pipeline')