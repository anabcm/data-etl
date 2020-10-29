import os

# add translations column
os.system('python translate.py')

os.system('bamboo-cli --folder . --entry programs')
