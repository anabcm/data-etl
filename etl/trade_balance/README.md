### Trade Balance ETL

#### Source: Secretary of Economy

#### Usage:
bash console = `source product_ingest.sh`
bash console = `trade_ingest.sh`

#### Sample data:
| year | export_value | import_num_plants | export_num_plants | import_value | mun_id | country_id | product_id | pci |
| :------------- | :----------: | -----------: | -----------: | -----------: | -----------: | -----------: | -----------: | -----------: |
| 2007 | 1465.0776 | 0 | 1 | 0 | 1001 | esp | 106 | -1.5311 |
| 2013 | 0 | 1 | 0 | 1850 | 1001  | zaf  | 106 | -1.279 |

#### Dimension tables
| product_name | product_name_es | product_id |
| :------------- | :----------: | -----------: | 
| Other articles of iron or steel | Las demás manufacturas de hierro o acero. | 7326 |
| Other articles of glass: | Las demás manufacturas de vidrio. | 7020 |