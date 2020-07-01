import unidecode

# rename
STATE_REPLACE = {
    "México": "Estado de México"
}

# append
MISSING_STATE = [
    {
        "cve_ent": "33",
        "ent_name": "No Informado",
        "ent_id": 33,
        "nation_name": "México",
        "nation_id": "mex"
    }
]

MISSING_MUNICIPALITY = [
    {
        "cve_ent": "33",
        "cve_mun": "000",
        "cve_mun_full": "33000",
        "ent_name": "No Informado",
        "mun_name": "No Informado",
        "ent_id": 33,
        "mun_id": 33000,
        "nation_name": "México",
        "nation_id": "mex",
    }
]

# functions
def slug_parser(txt):
    slug = txt.lower().replace(" ", "-")
    slug = unidecode.unidecode(slug)

    for char in ["]", "[", "(", ")"]:
        slug = slug.replace(char, "")

    return slug