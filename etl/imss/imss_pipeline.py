import pandas as pd

# IMSS
df = pd.read_csv("asg-2019-12-31.csv", low_memory=False, encoding="latin-1", delimiter="|", chunksize=10**5)
df = pd.concat(df)
df = df.drop(columns=["cve_subdelegacion", "sector_economico_1", "sector_economico_2", 
        "cve_delegacion", "masa_sal_teu", "masa_sal_tec", "masa_sal_tec", "masa_sal_tpc",
        "masa_sal_tpu", "masa_sal_tpc", "ta_sal", "teu_sal", "tec_sal", "tpu_sal", "tpc_sal"])
df = df.rename(columns={"rango_edad": "age_range", "cve_municipio": "mun_id_imss", "sexo": "sex", 
                         "tamaño_patron": "pattern_size", "rango_uma": "uma_range", "no_trabajadores": "non_workers",
                         "rango_salarial": "salary_range", "cve_entidad": "State ID"})
df["pattern_size"] = df["pattern_size"].fillna(0)
df["sector_economico_4"] = df["sector_economico_4"].fillna(0)
df["age_range"] = df["age_range"].fillna(0)
df["salary_range"] = df["salary_range"].fillna(0)
df["uma_range"] = df["uma_range"].fillna(0)
df["sex"] = df["sex"].fillna(0)
df["mun_id_imss"] = df["mun_id_imss"].fillna("000")
df["mun_id_imss"] = df["mun_id_imss"].astype(str) + df["State ID"].astype(str) 
df = df.drop(columns=["State ID"])
df["count"] = 1
df["age_range"] = df["age_range"].str.replace("E", "")
df["uma_range"] = df["uma_range"].str.replace("W", "")
df["pattern_size"] = df["pattern_size"].str.replace("S", "")
df["salary_range"] = df["salary_range"].str.replace("W", "")
df[["pattern_size", "salary_range", "uma_range", "sex", "age_range", "sector_economico_4"]] = df[["pattern_size",
                    "salary_range", "uma_range", "sex", "age_range", "sector_economico_4"]].astype(int)
df = df.groupby(["age_range", "mun_id_imss", "salary_range", "sector_economico_4",
                 "sex", "pattern_size", "uma_range"]).sum().reset_index()
df["salary"] = df["masa_sal_ta"] / df3["count"]
df["date"] = 201912

# Data dictonary from IMSS
df1 = pd.read_excel(open("diccionario_de_datos_1.xlsx", "rb"),
              sheet_name="entidad-municipio", header=1) 

# Data Mexico DW
df2 = pd.read_csv("Municipality.csv")
df2.drop(["Population", "Nation", "Nation ID"], axis=1, inplace=True)
df1 = df1.rename(columns={"cve_municipio": "mun_id_imss",
                          "descripción municipio": "Municipality", 
                          "cve_entidad": "State ID", 
                          "descripción entidad": "State"})
df2 = df2.rename(columns={"Municipality ID": "mun_id"})
df1["Municipality"] = df1["Municipality"].replace({
    "Batopilas": "Batopilas de Manuel Gómez Morín", 
    "Dolores Hidalgo Cuna de la Independencia": "Dolores Hidalgo Cuna de la Independencia Nacional", 
    "Jonacatepec": "Jonacatepec de Leandro Valle", 
    "Villa de Tututepec de Melchor Ocampo": "Villa de Tututepec", 
    "Heroica Villa Tezoatlán de Segura y Luna": "Heroica Villa Tezoatlán de Segura y Luna, Cuna de la Independencia de Oaxaca",
    "Isla de Cedros": "Ensenada", "Santa Ana Pacueco": "Pénjamo"})
df1.drop(["cve_delegacion", "State"], axis=1, inplace=True)

# Dictonary IMSS-DMX
df_dic = pd.merge(df1, df2, on=["Municipality", "State ID"], how="outer")
df_dic["mun_id_imss"].astype(str)
df_dic["mun_id"].astype(str)
df_dic["mun_id_imss"] = df_dic["mun_id_imss"].fillna("000")
df_dic["mun_id"] = df_dic["mun_id"].fillna("000")
df_dic["mun_id_imss"]  = df_dic["State ID"].astype(str) + df_dic["mun_id_imss"].astype(str) 
new_row = {"mun_id_imss":"9000", "State ID" : "0", "Municipality" : "0", "State" : "0", "mun_id" : "9000"}
df_dic = df_dic[df_dic.mun_id_imss != "9000"]
df_dic = df_dic.append(new_row, ignore_index="true")

# Data frame IMSS with DMX ID
df_final = pd.merge(df, df_dic, on=["mun_id_imss"], how="left"))
df_final= df_final.drop(columns=["Municipality", "mun_id_imss", "State", "State ID"])