### Credits ETL

#### Source: Secretary of Economy

#### Usage:
- `python households.py`
- `python imss_credits.py`
- `python wellness.py`

#### Sample data:
- Households
```
┌─ent_id─┬─mun_id─┬─level────────┬─sex─┬─person_type─┬─age_range─┬─count─┐
│      0 │   1001 │ Municipality │   1 │           1 │         5 │     5 │
│      0 │   1001 │ Municipality │   1 │           1 │         6 │    12 │
│      0 │   1001 │ Municipality │   1 │           1 │         7 │    41 │
│      0 │   1001 │ Municipality │   1 │           1 │         8 │    54 │
│      0 │   1001 │ Municipality │   1 │           1 │         9 │    74 │
└────────┴────────┴──────────────┴─────┴─────────────┴───────────┴───────┘
```
- IMSS
```
┌─ent_id─┬─mun_id─┬─level────────┬─sex─┬─person_type─┬─company_size─┬─age_range─┬─count─┐
│      0 │   1001 │ Municipality │   1 │           1 │            1 │         4 │     2 │
│      0 │   1001 │ Municipality │   1 │           1 │            1 │         5 │    72 │
│      0 │   1001 │ Municipality │   1 │           1 │            1 │         6 │   257 │
│      0 │   1001 │ Municipality │   1 │           1 │            1 │         7 │   295 │
│      0 │   1001 │ Municipality │   1 │           1 │            1 │         8 │   241 │
└────────┴────────┴──────────────┴─────┴─────────────┴──────────────┴───────────┴───────┘
```
- Wellness
```
┌─ent_id─┬─mun_id─┬─sex─┬─person_type─┬─age_range─┬─count─┬─level────────┐
│      1 │   1001 │   1 │           1 │         4 │     2 │ Municipality │
│      1 │   1001 │   1 │           1 │         5 │   448 │ Municipality │
│      1 │   1001 │   1 │           1 │         6 │   811 │ Municipality │
│      1 │   1001 │   1 │           1 │         7 │  1035 │ Municipality │
│      1 │   1001 │   1 │           1 │         8 │   952 │ Municipality │
└────────┴────────┴─────┴─────────────┴───────────┴───────┴──────────────┘
```