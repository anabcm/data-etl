
def match_last_char(string):
    if string[-1] == '.':
        temp = string[:-1]
        return temp
    else:
        return string

def format_text(df, cols_names=None, stopwords=None):
    # format
    for ele in cols_names:
        df[ele] = df[ele].str.title().str.strip()

        # remove punctuation
        df[ele] = df[ele].apply(lambda x: match_last_char(x))
        for ene in list(stopwords) + ['u']:
            df[ele] = df[ele].str.replace(' ' + ene.title() + ' ', ' ' + ene + ' ')

    return df

def fill_values(df, target, base):
    """fill nan values with another column where there are values"""
    mask = df[target].isnull()
    df.loc[mask, target] = df.loc[mask, base]
    return df

COUNTRIES = [
    {"iso2": "xx",
    "iso3": "xxa",
    "country_name": "Unknown",
    "country_name_es": "S/N",
    "continent_id": "x",
    "continent": "Unknown",
    "continent_es": "S/N",
    "id_num": "899",
    "oecd": 0},

    {"iso2": "bw",
    "iso3": "bwa",
    "country_name": "Botswana",
    "country_name_es": "Botswana",
    "continent_id": "af",
    "continent": "Africa",
    "continent_es": "África",
    "id_num": "72",
    "oecd": 0},

    {"iso2": "vi",
    "iso3": "vir",
    "country_name": "Virgin Islands",
    "country_name_es": "Islas Vírgenes",
    "continent_id": "na",
    "continent": "North America",
    "continent_es": "América del Norte",
    "id_num": "850",
    "oecd": 0},

    {"iso2": "sz",
    "iso3": "swz",
    "country_name": "Eswatini",
    "country_name_es": "Esuatini",
    "continent_id": "af",
    "continent": "Africa",
    "continent_es": "África",
    "id_num": "748",
    "oecd": 0},

    {"iso2": "fo",
    "iso3": "fro",
    "country_name": "Faroe Islands",
    "country_name_es": "Islas Feroe",
    "continent_id": "eu",
    "continent": "Europe",
    "continent_es": "Europa",
    "id_num": "234",
    "oecd": 0},

    {"iso2": "pr",
    "iso3": "pri",
    "country_name": "Puerto Rico",
    "country_name_es": "Puerto Rico",
    "continent_id": "na",
    "continent": "North America",
    "continent_es": "América del Norte",
    "id_num": "630",
    "oecd": 0},

    {"iso2": "na",
    "iso3": "nam",
    "country_name": "Namibia",
    "country_name_es": "Namibia",
    "continent_id": "af",
    "continent": "Africa",
    "continent_es": "África",
    "id_num": "516",
    "oecd": 0},

    {"iso2": "mc",
    "iso3": "mco",
    "country_name": "Monaco",
    "country_name_es": "Mónaco",
    "continent_id": "eu",
    "continent": "Europe",
    "continent_es": "Europa",
    "id_num": "492",
    "oecd": 0},

    {"iso2": "li",
    "iso3": "lie",
    "country_name": "Liechtenstein",
    "country_name_es": "Liechtenstein",
    "continent_id": "eu",
    "continent": "Europe",
    "continent_es": "Europa",
    "id_num": "438|1411	",
    "oecd": 0},

    {"iso2": "ls",
    "iso3": "lso",
    "country_name": "Lesotho",
    "country_name_es": "Lesotho",
    "continent_id": "af",
    "continent": "Africa",
    "continent_es": "África",
    "id_num": "426",
    "oecd": 0},

    {"iso2": "re",
    "iso3": "reu",
    "country_name": "Reunion",
    "country_name_es": "Réunion",
    "continent_id": "af",
    "continent": "Africa",
    "continent_es": "África",
    "id_num": "638",
    "oecd": 0},

    {"iso2": "ks",
    "iso3": "ksv",
    "country_name": "Kosovo",
    "country_name_es": "Kosovo",
    "continent_id": "eu",
    "continent": "Europe",
    "continent_es": "Europa",
    "id_num": "412",
    "oecd": 0},

    {"iso2": "im",
    "iso3": "imn",
    "country_name": "Isle of Man",
    "country_name_es": "Isla de Man",
    "continent_id": "eu",
    "continent": "Europe",
    "continent_es": "Europa",
    "id_num": "833",
    "oecd": 0},

    {"iso2": None,
    "iso3": "chi",
    "country_name": "Channel Islands",
    "country_name_es": "Islas del Canal",
    "continent_id": "eu",
    "continent": "Europe",
    "continent_es": "Europa",
    "id_num": "",
    "oecd": 0},

    {"iso2": "1w",
    "iso3": "wld",
    "country_name": "World",
    "country_name_es": "Mundo",
    "continent_id": "xx",
    "continent": None,
    "continent_es": None,
    "id_num": None,
    "oecd": 0}
]