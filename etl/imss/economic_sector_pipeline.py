import pandas as pd

# Data Dictonary from IMSS
xls = pd.ExcelFile("diccionario_de_datos_1.xlsx")

# Economic sector 1 ,2 and 4 Sheets from Data dictonary from IMSS
df1 = pd.read_excel(xls, "sector 1", header=1)
df2 = pd.read_excel(xls, "sector 2", header=1)
df3 = pd.read_excel(xls, "sector 4", header=1)
df = pd.merge(df1, df2, on=["sector_economico_1"])
df = pd.merge(df, df3, on=["sector_economico_1", "sector_economico_2_2pos"])
df = df.drop(columns=["sector_economico_2_x", "sector_economico_2_y", "sector_economico_4"])
df = df.rename(columns={"sector_economico_1" : "level_1_id", "sector_economico_2_2pos" : "level_2_id", "sector_economico_4_4_pos" : 
                        "level_4_id", "descripción sector_economico_1": "level_1_name", "descripción sector_economico_2" : "level_2_name", 
                        "descripción sector_economico_4" : "level_4_name"})
df["level_2_id"] = df["level_2_id"].astype(str).str.zfill(2)
df["level_4_id"] = df["level_4_id"].astype(str).str.zfill(2)