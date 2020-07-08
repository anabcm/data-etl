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
        "cve_ent": "1",
        "cve_mun": "999",
        "cve_mun_full": "1999",
        "ent_name": "Aguascalientes",
        "mun_name": "No Informado",
        "ent_id": 1,
        "mun_id": 1999,
        "nation_name": "México",
        "nation_id": "mex"
    },
    {
        "cve_ent": "2",
        "cve_mun": "999",
        "cve_mun_full": "2999",
        "ent_name": "Baja California",
        "mun_name": "No Informado",
        "ent_id": 2,
        "mun_id": 2999,
        "nation_name": "México",
        "nation_id": "mex"
    },
    {
        "cve_ent": "3",
        "cve_mun": "999",
        "cve_mun_full": "3999",
        "ent_name": "Baja California Sur",
        "mun_name": "No Informado",
        "ent_id": 3,
        "mun_id": 3999,
        "nation_name": "México",
        "nation_id": "mex"
    },
    {
        "cve_ent": "4",
        "cve_mun": "999",
        "cve_mun_full": "4999",
        "ent_name": "Campeche",
        "mun_name": "No Informado",
        "ent_id": 4,
        "mun_id": 4999,
        "nation_name": "México",
        "nation_id": "mex"
    },
    {
        "cve_ent": "5",
        "cve_mun": "999",
        "cve_mun_full": "5999",
        "ent_name": "Coahuila de Zaragoza",
        "mun_name": "No Informado",
        "ent_id": 5,
        "mun_id": 5999,
        "nation_name": "México",
        "nation_id": "mex"
    },
    {
        "cve_ent": "6",
        "cve_mun": "999",
        "cve_mun_full": "6999",
        "ent_name": "Colima",
        "mun_name": "No Informado",
        "ent_id": 6,
        "mun_id": 6999,
        "nation_name": "México",
        "nation_id": "mex"
    },
    {
        "cve_ent": "7",
        "cve_mun": "999",
        "cve_mun_full": "7999",
        "ent_name": "Chiapas",
        "mun_name": "No Informado",
        "ent_id": 7,
        "mun_id": 7999,
        "nation_name": "México",
        "nation_id": "mex"
    },
    {
        "cve_ent": "8",
        "cve_mun": "999",
        "cve_mun_full": "8999",
        "ent_name": "Chihuahua",
        "mun_name": "No Informado",
        "ent_id": 8,
        "mun_id": 8999,
        "nation_name": "México",
        "nation_id": "mex"
    },
    {
        "cve_ent": "9",
        "cve_mun": "106",
        "cve_mun_full": "9106",
        "ent_name": "Ciudad de México",
        "mun_name": "No Informado",
        "ent_id": 9,
        "mun_id": 9106,
        "nation_name": "México",
        "nation_id": "mex"
    },
    {
        "cve_ent": "9",
        "cve_mun": "999",
        "cve_mun_full": "9999",
        "ent_name": "Ciudad de México",
        "mun_name": "No Informado",
        "ent_id": 9,
        "mun_id": 9999,
        "nation_name": "México",
        "nation_id": "mex"
    },
    {
        "cve_ent": "10",
        "cve_mun": "999",
        "cve_mun_full": "10999",
        "ent_name": "Durango",
        "mun_name": "No Informado",
        "ent_id": 10,
        "mun_id": 10999,
        "nation_name": "México",
        "nation_id": "mex"
    },
    {
        "cve_ent": "11",
        "cve_mun": "999",
        "cve_mun_full": "11999",
        "ent_name": "Guanajuato",
        "mun_name": "No Informado",
        "ent_id": 11,
        "mun_id": 11999,
        "nation_name": "México",
        "nation_id": "mex"
    },
    {
        "cve_ent": "12",
        "cve_mun": "999",
        "cve_mun_full": "12999",
        "ent_name": "Guerrero",
        "mun_name": "No Informado",
        "ent_id": 12,
        "mun_id": 12999,
        "nation_name": "México",
        "nation_id": "mex"
    },
    {
        "cve_ent": "13",
        "cve_mun": "999",
        "cve_mun_full": "13999",
        "ent_name": "Hidalgo",
        "mun_name": "No Informado",
        "ent_id": 13,
        "mun_id": 13999,
        "nation_name": "México",
        "nation_id": "mex"
    },
    {
        "cve_ent": "14",
        "cve_mun": "999",
        "cve_mun_full": "14999",
        "ent_name": "Jalisco",
        "mun_name": "No Informado",
        "ent_id": 14,
        "mun_id": 14999,
        "nation_name": "México",
        "nation_id": "mex"
    },
    {
        "cve_ent": "15",
        "cve_mun": "999",
        "cve_mun_full": "15999",
        "ent_name": "Estado de México",
        "mun_name": "No Informado",
        "ent_id": 15,
        "mun_id": 15999,
        "nation_name": "México",
        "nation_id": "mex"
    },
    {
        "cve_ent": "16",
        "cve_mun": "999",
        "cve_mun_full": "16999",
        "ent_name": "Michoacán de Ocampo",
        "mun_name": "No Informado",
        "ent_id": 16,
        "mun_id": 16999,
        "nation_name": "México",
        "nation_id": "mex"
    },
    {
        "cve_ent": "17",
        "cve_mun": "053",
        "cve_mun_full": "17053",
        "ent_name": "Morelos",
        "mun_name": "No Informado",
        "ent_id": 17,
        "mun_id": 17053,
        "nation_name": "México",
        "nation_id": "mex"
    },
    {
        "cve_ent": "17",
        "cve_mun": "999",
        "cve_mun_full": "17056",
        "ent_name": "Morelos",
        "mun_name": "No Informado",
        "ent_id": 17,
        "mun_id": 17056,
        "nation_name": "México",
        "nation_id": "mex"
    },
    {
        "cve_ent": "17",
        "cve_mun": "063",
        "cve_mun_full": "17063",
        "ent_name": "Morelos",
        "mun_name": "No Informado",
        "ent_id": 17,
        "mun_id": 17063,
        "nation_name": "México",
        "nation_id": "mex"
    },
    {
        "cve_ent": "17",
        "cve_mun": "053",
        "cve_mun_full": "17999",
        "ent_name": "Morelos",
        "mun_name": "No Informado",
        "ent_id": 17,
        "mun_id": 17999,
        "nation_name": "México",
        "nation_id": "mex"
    },
    {
        "cve_ent": "18",
        "cve_mun": "053",
        "cve_mun_full": "18999",
        "ent_name": "Nayarit",
        "mun_name": "No Informado",
        "ent_id": 18,
        "mun_id": 18999,
        "nation_name": "México",
        "nation_id": "mex"
    },
    {
        "cve_ent": "19",
        "cve_mun": "066",
        "cve_mun_full": "19066",
        "ent_name": "Nuevo León",
        "mun_name": "No Informado",
        "ent_id": 19,
        "mun_id": 19066,
        "nation_name": "México",
        "nation_id": "mex"
    },
    {
        "cve_ent": "19",
        "cve_mun": "053",
        "cve_mun_full": "19999",
        "ent_name": "Nuevo León",
        "mun_name": "No Informado",
        "ent_id": 19,
        "mun_id": 19999,
        "nation_name": "México",
        "nation_id": "mex"
    },
    {
        "cve_ent": "20",
        "cve_mun": "999",
        "cve_mun_full": "20999",
        "ent_name": "Oaxaca",
        "mun_name": "No Informado",
        "ent_id": 20,
        "mun_id": 20999,
        "nation_name": "México",
        "nation_id": "mex"
    },
    {
        "cve_ent": "21",
        "cve_mun": "999",
        "cve_mun_full": "21999",
        "ent_name": "Puebla",
        "mun_name": "No Informado",
        "ent_id": 21,
        "mun_id": 21999,
        "nation_name": "México",
        "nation_id": "mex"
    },
    {
        "cve_ent": "22",
        "cve_mun": "999",
        "cve_mun_full": "Querétaro",
        "ent_name": "No Informado",
        "mun_name": "No Informado",
        "ent_id": 22,
        "mun_id": 22999,
        "nation_name": "México",
        "nation_id": "mex"
    },
    {
        "cve_ent": "23",
        "cve_mun": "999",
        "cve_mun_full": "23999",
        "ent_name": "Quintana Roo",
        "mun_name": "No Informado",
        "ent_id": 23,
        "mun_id": 23999,
        "nation_name": "México",
        "nation_id": "mex"
    },
    {
        "cve_ent": "24",
        "cve_mun": "999",
        "cve_mun_full": "24999",
        "ent_name": "San Luis Potosí",
        "mun_name": "No Informado",
        "ent_id": 24,
        "mun_id": 24999,
        "nation_name": "México",
        "nation_id": "mex"
    },
    {
        "cve_ent": "25",
        "cve_mun": "999",
        "cve_mun_full": "25999",
        "ent_name": "Sinaloa",
        "mun_name": "No Informado",
        "ent_id": 25,
        "mun_id": 25999,
        "nation_name": "México",
        "nation_id": "mex"
    },
    {
        "cve_ent": "26",
        "cve_mun": "999",
        "cve_mun_full": "26999",
        "ent_name": "Sonora",
        "mun_name": "No Informado",
        "ent_id": 26,
        "mun_id": 26999,
        "nation_name": "México",
        "nation_id": "mex"
    },
    {
        "cve_ent": "27",
        "cve_mun": "999",
        "cve_mun_full": "27999",
        "ent_name": "Tabasco",
        "mun_name": "No Informado",
        "ent_id": 27,
        "mun_id": 27999,
        "nation_name": "México",
        "nation_id": "mex"
    },
    {
        "cve_ent": "28",
        "cve_mun": "999",
        "cve_mun_full": "28999",
        "ent_name": "Tamaulipas",
        "mun_name": "No Informado",
        "ent_id": 28,
        "mun_id": 28999,
        "nation_name": "México",
        "nation_id": "mex"
    },
    {
        "cve_ent": "29",
        "cve_mun": "999",
        "cve_mun_full": "29999",
        "ent_name": "Tlaxcala",
        "mun_name": "No Informado",
        "ent_id": 29,
        "mun_id": 29999,
        "nation_name": "México",
        "nation_id": "mex"
    },
    {
        "cve_ent": "30",
        "cve_mun": "999",
        "cve_mun_full": "30999",
        "ent_name": "Veracruz de Ignacio de la Llave",
        "mun_name": "No Informado",
        "ent_id": 30,
        "mun_id": 30999,
        "nation_name": "México",
        "nation_id": "mex"
    },
    {
        "cve_ent": "31",
        "cve_mun": "999",
        "cve_mun_full": "31999",
        "ent_name": "Yucatán",
        "mun_name": "No Informado",
        "ent_id": 31,
        "mun_id": 31999,
        "nation_name": "México",
        "nation_id": "mex"
    },
    {
        "cve_ent": "32",
        "cve_mun": "999",
        "cve_mun_full": "32999",
        "ent_name": "Zacatecas",
        "mun_name": "No Informado",
        "ent_id": 32,
        "mun_id": 32999,
        "nation_name": "México",
        "nation_id": "mex"
    },
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
    },
    {
        "cve_ent": "36",
        "cve_mun": "999",
        "cve_mun_full": "36999",
        "ent_name": "No Informado",
        "mun_name": "No Informado",
        "ent_id": 36,
        "mun_id": 36999,
        "nation_name": "México",
        "nation_id": "mex"
    }
]

# functions
def slug_parser(txt):
    slug = txt.lower().replace(" ", "-")
    slug = unidecode.unidecode(slug)

    for char in ["]", "[", "(", ")"]:
        slug = slug.replace(char, "")

    return slug