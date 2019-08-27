import os

data = ['https://storage.googleapis.com/datamexico-data/anuies/postgraduate/posgrado_2016-2017.xlsx',
        'https://storage.googleapis.com/datamexico-data/anuies/postgraduate/posgrado_2017-2018.xlsx']

cmd = ['bamboo-cli --folder . --entry enrollment_pipeline', 
       'bamboo-cli --folder . --entry origin_pipeline', 
       'bamboo-cli --folder . --entry status_pipeline']

# data
for url in data:
  for command in cmd:
    os.system(command + ' --url=' + url)

# work center index
data = ['https://storage.googleapis.com/datamexico-data/anuies/work_center/index_work_center_posgrado_2016-2017.xlsx',
        'https://storage.googleapis.com/datamexico-data/anuies/work_center/index_work_center_posgrado_2017-2018.xlsx']
for url in data:
  os.system('bamboo-cli --folder . --entry universities_index_pipeline --url=' + url)

# careers index
os.system('bamboo-cli --folder . --entry careers_programs')

# campus index
os.system('bamboo-cli --folder . --entry campus_index_pipeline')