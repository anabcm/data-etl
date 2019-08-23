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
for ent in range(1,33):
  os.system('bamboo-cli --folder . --entry work_centers_pipeline --index=' + str(ent))

# careers index
os.system('bamboo-cli --folder . --entry careers_programs')