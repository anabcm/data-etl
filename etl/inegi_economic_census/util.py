import pandas as pd 

def format_text(df, cols_names=None, stopwords=None):
    # format
    for ele in cols_names:
        df[ele] = df[ele].str.title().str.strip()
        for ene in stopwords:
            df[ele] = df[ele].str.replace(' ' + ene.title() + ' ', ' ' + ene + ' ')

    return df


#Functions indicators economic census 2019
def general_format(df,a,b, column_name, fix_subsector, rows_drop):
    df = df.iloc[a:]
    df = df[:-b]
    df.columns = column_name
    
    df = df.loc[df["ent_id"] == "00 NAL"]
    df = df.assign(category=lambda d: d["category"].str.strip().str.replace(" +", " "))
    for i in rows_drop:
        df = df[~df["category"].str.contains(i)]
    df = df[df["sector_id"].notna()]
    df["sector_id"] = df["sector_id"].str.replace("Sector ", "")
    df = df.assign(sector_id=lambda d: d["sector_id"].str.strip().str.replace(" +", " "))
    df["subsector_id"] = df["subsector_id"].str.replace(fix_subsector, "")
    df = df.assign(subsector_id=lambda d: d["subsector_id"].str.strip().str.replace(" +", " "))
    df["subsector_id"] = df["subsector_id"].fillna(0)
    df.loc[df['subsector_id'] != 0, 'sector_id'] = 0
    df["ent_id"] = 0
    df["nation_id"] = 1
    
    return df


def yes_no_format(df,a,b):
    data = []
    for i in [a, b]:
        df_ = df[["nation_id", "ent_id", "sector_id", "subsector_id", "category", "value_"+i, "pct_"+i]]
        df_["indicator"] = i
        df_ = df_.rename(columns={"value_"+i: "value", "pct_"+i: "percentage"})
        data.append(df_)
    df_ = pd.concat(data, sort=False)
    
    return df_


def categories_format(df, names):
    dfA = df[["nation_id", "ent_id", "sector_id", "subsector_id", "category"] + [s for s in names if "_value" in s]]
    dfA = dfA.melt(id_vars=["nation_id", "ent_id", "sector_id", "subsector_id", "category"], var_name="indicator", value_name="value")
    
    dfB = df[["nation_id", "ent_id", "sector_id", "subsector_id", "category"]+ [s for s in names if "_pct" in s]]
    dfB.columns = dfB.columns.str.replace("_pct", "_value")
    dfB = dfB.melt(id_vars=["nation_id", "ent_id", "sector_id", "subsector_id", "category"], var_name="indicator", value_name="percentage")

    dfC = pd.merge(dfA, dfB, on=["nation_id", "ent_id", "sector_id", "subsector_id", "category", "indicator"])
    
    return dfC



def geo_data(df, selected_columns, cut1, cut2):
    df = df.iloc[cut1:]
    df = df[:-cut2]

    column_number = len(df.columns) + 1
    df.columns=["Column"+str(i) for i in range(1, column_number)]

    df = df[selected_columns]
    df.columns = ["ent_id", "sector_id", "subsector_id", "category", "value", "percentage"]

    df = df[df["sector_id"].notna()]
    df = df[~df["category"].str.contains("personas")]
    df = df[~df["category"].str.contains("Todos")]
    df = df[~df["category"].str.contains("Rama")]
    
    df["sector_id"] = df["sector_id"].str.replace("Sector ", "")
    df = df.assign(sector_id=lambda d: d["sector_id"].str.strip().str.replace(' +', ' '))
    df["subsector_id"] = df["subsector_id"].str.replace("Subsector ", "")
    
    df["subsector_id"] = df["subsector_id"].fillna(0)
    df.loc[df["subsector_id"] != 0, "sector_id"] = 0

    df["ent_id"] = df["ent_id"].str[0:2].astype(int)
    df["nation_id"] = df["ent_id"].apply(lambda x: 1 if x == 0 else 0)
    df["category"] = "No aplica"

    return df

def environmental(df, a, b, column_name):
    df = df.iloc[a:]
    df = df[:-b]
    
    df.columns = column_name

    df = df[(df["ent_id"]=="00 NAL") | (df["sector_id"].isnull())]

    df["ent_id"] = df["ent_id"].str[0:2].astype(int)
    df["nation_id"] = df["ent_id"].apply(lambda x: 1 if x == 0 else 0)

    df["sector_id"] = df["sector_id"].str.replace("Sector ", "")
    df = df.assign(sector_id=lambda d: d["sector_id"].str.strip().str.replace(" +", " "))
    df["sector_id"] = df["sector_id"].fillna(0)
    df["subsector_id"] = 0
    df["category"] = "No aplica"
    
    return df