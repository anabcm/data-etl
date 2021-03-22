### Foreign Direct Investment ETL

#### Source: Secretary of Economy

#### Usage:
- to update, make sure you replace the correct files on GCP then run the following scripts:
```
python fdi_2.py
python fdi_3.py
python fdi_4.py
python fdi_9.py
python fdi_10.py
python fdi_aggregate_accumulated.py
python fdi_aggregate_country_ent.py
python fdi_aggregate_geo_investment_type.py
python fdi_aggregate_investment_type.py
python fdi_aggregate_top3.py
python python run_fdi_additional_tables.py
```

#### Sample data:
| generic_investment | origin_id | area_id | ent_id | value_million | count | quarter_id |
| :------------- | :----------: | -----------: | -----------: | -----------: | -----------: | -----------: |
| 2 | chl | 52230 | 1 | -0.017309 | 1 | 2004 |
| 1 | chl | 5223 | 1 | 0.005982 | 1  | 20112 |
