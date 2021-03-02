### ANUIES - Asociación Nacional de Universidades e Instituciones de Educación Superior

#### Source: http://www.anuies.mx/informacion-y-servicios/informacion-estadistica-de-educacion-superior/anuario-estadistico-de-educacion-superior

#### Usage:
`python dim_institutions.py` \
`python dim_careers.py` \
`python origin_pipeline.py` \
`python enrollment_pipeline.py` \
`python status_pipeline.py`

#### Sample data:
| mun_id | type | campus_id | program | origin | value | year |
| :------------- | :----------: | -----------: | -----------: | -----------: | -----------: |  -----------: | 
| 1001 | 11 | 100001 | 420000914 | 1 | 71 | 2017 |
| 1001 | 11 | 100002 | 420000914 | 9 | 1 | 2017 |
| 1001 | 11 | 200001 | 420000914 | 14 | 1 | 2017 |

#### Util:
Inside **notebooks** folder, jupyter notebooks with ids creation scripts, you will need all data files on that folder.

`ANUIES_careers_dim.ipynb` \
`ANUIES_institutions_dim.ipynb`