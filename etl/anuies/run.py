import os

data = ['https://storage.googleapis.com/datamexico-data/anuies/licenciatura_2016-2017.xlsx',
        'https://storage.googleapis.com/datamexico-data/anuies/licenciatura_2017-2018.xlsx',
        'https://storage.googleapis.com/datamexico-data/anuies/licenciatura_2018-2019.xlsx',
        'https://storage.googleapis.com/datamexico-data/anuies/posgrado_2016-2017.xlsx',
        'https://storage.googleapis.com/datamexico-data/anuies/posgrado_2017-2018.xlsx',
        'https://storage.googleapis.com/datamexico-data/anuies/posgrado_2018-2019.xlsx']

cmd = ['bamboo-cli --folder . --entry enrollment_pipeline', 
       'bamboo-cli --folder . --entry origin_pipeline', 
       'bamboo-cli --folder . --entry status_pipeline']

for url in data:
  # work center index
  os.system('bamboo-cli --folder . --entry index_work_centers --url={}'.format(url))
  # careers index
  os.system('bamboo-cli --folder . --entry index_careers --url={}'.format(url))

# add translations column
os.system('python translate.py')

os.system('bamboo-cli --folder . --entry programs')

# data
for year in range(2016, 2019):
  for command in cmd:
    os.system(('{} --url=https://storage.googleapis.com/datamexico-data/anuies/licenciatura_{}-{}.xlsx --period={}').format(command, str(year), str(year+1), year+1))

for year in range(2016, 2019):
  for command in cmd:
    os.system(('{} --url=https://storage.googleapis.com/datamexico-data/anuies/posgrado_{}-{}.xlsx --period={}').format(command, str(year), str(year+1), year+1))