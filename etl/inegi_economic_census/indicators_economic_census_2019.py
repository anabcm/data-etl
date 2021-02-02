import pandas as pd
import numpy as np
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, PipelineStep, Parameter
from bamboo_lib.steps import DownloadStep, LoadStep
from util import general_format, yes_no_format, categories_format, geo_data, environmental


class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        dim_categories = pd.read_csv(prev[0], sep=";", encoding="utf-8").to_dict(orient="list")
        dim_indicators = pd.read_csv(prev[1], sep=";", encoding="utf-8").to_dict(orient="list")
        capance19_02 = pd.read_excel(prev[2])
        contnce19_02 = pd.read_excel(prev[3])
        crednce19_03 = pd.read_excel(prev[4])
        crednce19_04 = pd.read_excel(prev[5])
        crednce19_05 = pd.read_excel(prev[6])
        crednce19_06 = pd.read_excel(prev[7])
        crednce19_07 = pd.read_excel(prev[8])
        crednce19_08 = pd.read_excel(prev[9])
        ecomnce19_03 = pd.read_excel(prev[10])
        ecomnce19_04 = pd.read_excel(prev[11])
        ecomnce19_05 = pd.read_excel(prev[12])
        ecomnce19_06 = pd.read_excel(prev[13])
        edadnce19_02 = pd.read_excel(prev[14])
        edadnce19_03 = pd.read_excel(prev[15])
        innonce19_01 = pd.read_excel(prev[16])
        innonce19_04 = pd.read_excel(prev[17])
        innonce19_06 = pd.read_excel(prev[18])
        medance19_01 = pd.read_excel(prev[19])
        medance19_03 = pd.read_excel(prev[20])
        medance19_05 = pd.read_excel(prev[21])
        medance19_07 = pd.read_excel(prev[22])
        medance19_08 = pd.read_excel(prev[23])
        medance19_09 = pd.read_excel(prev[24])
        pagonce19_02 = pd.read_excel(prev[25])
        probnce19_03 = pd.read_excel(prev[26])
        ticsnce19_01 = pd.read_excel(prev[27])
        ticsnce19_02 = pd.read_excel(prev[28])
        ticsnce19_03 = pd.read_excel(prev[29])
        ticsnce19_04 = pd.read_excel(prev[30])


        #Silence warning
        pd.options.mode.chained_assignment = None

        #Create dictionary for categories
        dicto_categories = dict(zip(dim_categories["category_temp_name"], dim_categories["category_id"]))

        #Create dictionary for indicators
        dicto_indicators = dict(zip(dim_indicators["indicator_temp_name"], dim_indicators["indicator_id"]))

        fix_names = ["nation_id", "ent_id", "sector_id", "subsector_id", "category"]
       # *********************************** FINANCING *******************************************
       #Financing by age and size
        data1 = []
        for df in [crednce19_03, crednce19_04, crednce19_05, crednce19_06]:

            name = "tamaño" if df.shape[0]<15000 else "edad"
            id_ = "size" if df.shape[0]<15000 else "age"
            cut = 8 if df.shape[0]<15000 else 9
            
            column_name_3 = ["ent_id", "sector_id", "subsector_id", "category_id", "category", "total_ue", "ue_with_financing", "pct_with_financing", id_+"_banks_value", id_+"_banks_pct", id_+"_popular_banks_value", id_+"_popular_banks_pct", id_+"_providers_value", id_+"_providers_pct", id_+"_friends_value", id_+"_friends_pct", id_+"_government_value", id_+"_government_pct", id_+"_loans_value", id_+"_loans_pct", id_+"_partners_value", id_+"_partners_pct", id_+"_others_value", id_+"_others_pct"]
            column_name_5 = ["ent_id", "sector_id", "subsector_id", "category_name", "category", "total_ue", "ue_with_financing", "pct_with_financing", id_+"_new_business_value", id_+"_new_business_pct", id_+"_equipment_value", id_+"_equipment_pct", id_+"_local_car_value", id_+"_local_car_pct", id_+"_debt_payment_value", id_+"_debt_payment_pct", id_+"_national_input_value", id_+"_national_input_pct", id_+"_foreign_input_value", id_+"_foreign_input_pct", id_+"_salaries_value", id_+"_salaries_pct", id_+"_no_specify_uses_value", id_+"_no_specify_uses_pct", "ind1", "ind2"]
            column_name = column_name_3 if df.shape[1]==24 else column_name_5

            df_crednce19_03 = df
            
            rows_drop = ["Todos", "Sector", "Subsector"]
            df_crednce19_03 = general_format(df_crednce19_03, cut, 15, column_name, "Subsector", rows_drop)
            
            if (df.shape[1]==24):
                df_crednce19_03["indicator"] = id_ + "_financing"
                df_crednce19_03 = df_crednce19_03.rename(columns={"ue_with_financing": "value", "pct_with_financing": "percentage"})
                df_crednce19_03["value_no_financing"] = df_crednce19_03["total_ue"] - df_crednce19_03["value"]
                df_crednce19_03["pct_no_financing"] = 100 - df_crednce19_03["percentage"]

                df_crednce19_03_B1 = df_crednce19_03[["nation_id", "ent_id", "sector_id", "subsector_id", "value", "percentage", "indicator", "category"]]
                df_crednce19_03_B2 = df_crednce19_03[["nation_id", "ent_id", "sector_id", "subsector_id", "value_no_financing", "pct_no_financing", "category"]]
                df_crednce19_03_B2["indicator"] = id_ + "_no_financing"
                df_crednce19_03_B2 = df_crednce19_03_B2.rename(columns={"value_no_financing": "value", "pct_no_financing": "percentage"})

            ## Economic Units by value and percentage: financing sources
            df_crednce19_03_B5 = categories_format(df_crednce19_03, column_name)

            frames = [df_crednce19_03_B1, df_crednce19_03_B2, df_crednce19_03_B5] if df.shape[1]==24 else [df_crednce19_03_B5]
            df_final = pd.concat(frames)

            data1.append(df_final)
            
        df_financing1 = pd.concat(data1, sort=False)


        #Values by nation and state
        data2 = []
        for df in [crednce19_03, crednce19_07, crednce19_08]:

            name_dict = {24: "financing", 26: "credit", 20: "bank"}
            name = name_dict[df.shape[1]]

            df_crednce19_07 = df

            selected_columns = ["Column1", "Column2", "Column3", "Column5", "Column7", "Column8"]
            df_crednce19_07 = geo_data(df_crednce19_07, selected_columns, 8, 15)
            df_crednce19_07["indicator"] = name + "_by_state"

            data2.append(df_crednce19_07)
            
        df_financing2 = pd.concat(data2, sort=False)


        #Reasons no credit/no bank
        data3 = []
        for df in [crednce19_07, crednce19_08]:
                                                                                                                                                                                                                                                                                                                                                                                                                                    
            column_name_7 = ["ent_id", "sector_id", "subsector_id", "category_id", "category", "total_ue", "value_ue_with_credit", "pct_ue_with_credit", "value_ue_no_credit", "pct_ue_no_credit", "no_requirement_value", "no_requirement_pct", "no_need_credit_value", "no_need_credit_pct", "high_interest_value", "high_interest_pct", "remoteness_value", "remoteness_pct", "ignorance_value", "ignorance_pct", "lack_of_trust_value", "lack_of_trust_pct", "no_grant_value", "no_grant_pct", "no_specify_credit_value", "no_specify_credit_pct"]
            column_name_8 = ["ent_id", "sector_id", "subsector_id", "category_id", "category", "total_ue", "value_ue_with_bank", "pct_ue_with_bank", "value_ue_no_bank", "pct_ue_no_bank", "process_ignorance_value", "process_ignorance_pct", "no_need_bank_value", "no_need_bank_pct", "high_commissions_value", "high_commissions_pct", "no_specify_bank_value", "no_specify_bank_pct", "ind1", "ind2"]
            
            column_name = column_name_7 if (df.shape[1]==26) else column_name_8
            category = "credit" if (df.shape[1]==26) else "bank"
            
            values_name = [s for s in column_name if "_value" in s]
            pct_name = [s for s in column_name if "_pct" in s]
            rows_drop = ["Todos", "Sector", "Subsector"]
            
            #df_crednce19_07 = df
            df_crednce19_07 = general_format(df, 8, 15, column_name, "Subsector", rows_drop)
            
            #Options si/no
            df_contnce19_02_CC = yes_no_format(df_crednce19_07, "ue_with_"+category, "ue_no_"+category)

            ## Economic Units by value and percentage: credits
            df_crednce19_07_C5 = categories_format(df_crednce19_07, column_name)
            
            frames = [df_contnce19_02_CC, df_crednce19_07_C5]
            df_final = pd.concat(frames)
            
            data3.append(df_final)

        df_financing3 = pd.concat(data3, sort=False)

        #Final df financing
        frames_final_financing = [df_financing1, df_financing2, df_financing3]
        df_financing = pd.concat(frames_final_financing)


        # *********************************** INTERNET *******************************************
        #Values/pct by sector
        data1 = []
        for df_ in [ecomnce19_05, ecomnce19_06]: 

            cut1 = 4 if (df_.shape[1]==22) else 1
            cut2 = 21 if (df_.shape[1]==22) else 18
            name = "purchase" if (df_.shape[1]==22) else "sale"

            df = df_.iloc[:,:-cut1]

            column_name = ["ent_id", "sector_id", "subsector_id", "category_id", "category", "total_ue", "ue_no_"+name, "ue_"+name, "total_expenses_"+name, "expenses_"+name, "internet_"+name, "internet_"+name+"_participation", "internet_"+name+"_participation_ue", "suppliers_page_"+name, "intermediary_web_pages_"+name, "social_networks_"+name, "email_"+name, "other_computer_modality_"+name]
            rows_drop = ["Todos", "Sector", "Subsector"]
            df = general_format(df, 7, cut2, column_name, "Subsector", rows_drop)   
            df = df.assign(sector_id=lambda d: d["sector_id"].str.strip().str.replace(" +", " "))

            df = df[fix_names + ["suppliers_page_"+name, "intermediary_web_pages_"+name, "social_networks_"+name, "email_"+name, "other_computer_modality_"+name]]
            df = df.melt(id_vars=fix_names, var_name="indicator", value_name="value")
            
            data1.append(df)
                
        df_internet_value = pd.concat(data1, sort=False)

        data2 = []
        for df_ in [ecomnce19_03, ecomnce19_04]: 
            name = "purchase" if (df_.isnull().sum().sum()<14610) else "sale"
            column_name = ["ent_id", "sector_id", "subsector_id", "category_id", "category", "total_ue", "no_"+name+"_value", "no_"+name+"_pct", name+"_value", name+"_pct", "suppliers_page_value_"+name, "suppliers_page_pct_"+name, "intermediary_web_pages_value_"+name, "intermediary_web_pages_pct_"+name, "social_networks_value_"+name, "social_networks_pct_"+name, "email_value_"+name, "email_pct_"+name, "other_computer_modality_value_"+name, "other_computer_modality_pct_"+name, "expenses", "value_internet_"+name, "pct_internet_"+name]
            rows_drop = ["Todos", "Sector", "Subsector"]
            df = general_format(df_, 7, 21, column_name, "Subsector", rows_drop) 

            df = df[fix_names + ["suppliers_page_pct_"+name, "intermediary_web_pages_pct_"+name, "social_networks_pct_"+name, "email_pct_"+name, "other_computer_modality_pct_"+name]]
            df.columns = df.columns.str.replace("pct_", "")
            df = df.melt(id_vars=fix_names, var_name="indicator", value_name="percentage")

            data2.append(df)
                
        df_internet_pct = pd.concat(data2, sort=False)
        df_internet1 = pd.merge(df_internet_value, df_internet_pct, on=["nation_id", "ent_id", "sector_id", "subsector_id", "category", "indicator"])


        #Geo values
        data3 = []
        for df_ in [ecomnce19_05, ecomnce19_06]: 
            name = "purchase" if (df_.shape[1]==22) else "sale"
            selected_columns = ["Column1", "Column2", "Column3", "Column5", "Column11", "Column12"]
            cut1 = 4 if (df_.shape[1]==22) else 1
            cut2 = 21 if (df_.shape[1]==22) else 18
            df = df_[:-cut2]
            
            df = geo_data(df, selected_columns, 7, cut1)
            df["indicator"] = name + "_by_state"
            
            data3.append(df)
                
        df_internet2 = pd.concat(data3, sort=False)

        #Final df internet
        frames_final_internet = [df_internet1, df_internet2]
        df_internet = pd.concat(frames_final_internet)


        # *********************************** BUSINESS MANAGEMENT *******************************************
        #Problems
        df_probnce19_03 = probnce19_03
        column_name = ["ent_id", "sector_id", "subsector_id", "category_id", "category", "total_ue", "value_no_problem", "pct_no_problem", "value_problem", "pct_problem", "credit_lack_value", "credit_lack_pct", "government_value", "government_pct", "taxes_value", "taxes_pct", "competition_value", "competition_pct", "informality_value", "informality_pct", "it_value", "it_pct", "demand_value", "demand_pct", "materials_value", "materials_pct", "services_payment_value", "services_payment_pct", "gob_payment_value", "gob_payment_pct", "staff_exp_value", "staff_exp_pct", "insecurity_value", "insecurity_pct", "corruption_value", "corruption_pct", "costs_value", "costs_pct", "other_problem_value", "other_problem_pct"]
        fix_subsector_ = "Subsector"

        #Problems by sector
        rows_drop = ["Todos", "Sector", "Subsector"]
        df_probnce19_03_A = general_format(df_probnce19_03, 7, 14, column_name, "Subsector", rows_drop)   
        df_probnce19_03_A = categories_format(df_probnce19_03_A, column_name) 
        #df_probnce19_03_A["nation_id"] = 1

        #Problems by state
        df_probnce19_03_B = df_probnce19_03.iloc[7:]
        df_probnce19_03_B = df_probnce19_03_B[:-14]

        df_probnce19_03_B.columns = column_name
            
        df_probnce19_03_B = df_probnce19_03_B[df_probnce19_03_B["category"].str.contains("personas")]
        df_probnce19_03_B = df_probnce19_03_B[df_probnce19_03_B["sector_id"].isnull()]
        for i in ["sector_id", "subsector_id"]:
            df_probnce19_03_B[i] = df_probnce19_03_B[i].fillna(0)

        df_probnce19_03_B["ent_id"] = df_probnce19_03_B["ent_id"].str[0:2].astype(int)
        df_probnce19_03_B["nation_id"] = df_probnce19_03_B["ent_id"].apply(lambda x: 1 if x == 0 else 0)
        df_probnce19_03_B = df_probnce19_03_B.assign(category=lambda d: d["category"].str.strip().str.replace(" +", " "))

        df_probnce19_03_B = categories_format(df_probnce19_03_B, column_name) 
        df_probnce19_03_B["indicator"] = df_probnce19_03_B["indicator"] + "_state"

        frames_business1 = [df_probnce19_03_A, df_probnce19_03_B]
        df_business1 = pd.concat(frames_business1)

        #Accounting
        df_contnce19_02 = contnce19_02
        column_name = ["ent_id", "sector_id", "subsector_id", "category_id", "category", "total_ue", "value_no_accounting", "pct_no_accounting", "value_accounting", "pct_accounting", "external_accounting_value", "external_accounting_pct", "own_accounting_value", "own_accounting_pct", "third_accounting_value", "third_accounting_pct"]

        rows_drop = ["Todos", "Sector", "Subector"]
        df_contnce19_02_A = general_format(df_contnce19_02, 8, 14, column_name, "Subector", rows_drop) 
        df_contnce19_02_A["subsector_id"] = df_contnce19_02_A["subsector_id"].str.replace("Subsector", "")
        df_contnce19_02_A = df_contnce19_02_A.assign(subsector_id=lambda d: d["subsector_id"].str.strip().str.replace(" +", " "))
        df_contnce19_02_A["subsector_id"] = df_contnce19_02_A["subsector_id"].fillna(0)

        #Yes/no options
        df_contnce19_02_AA = yes_no_format(df_contnce19_02_A, "accounting", "no_accounting")

        ## Economic Units by value and percentage: problems
        df_contnce19_02_A5 = categories_format(df_contnce19_02_A, column_name)

        #Accounting by state
        selected_columns = ["Column1", "Column2", "Column3", "Column5", "Column9", "Column10"]
        df_contnce19_02_B = geo_data(df_contnce19_02, selected_columns, 8, 14)
        df_contnce19_02_B["indicator"] = "accounting_system_by_state"

        frames_business2 = [df_contnce19_02_AA, df_contnce19_02_A5, df_contnce19_02_B]
        df_business2 = pd.concat(frames_business2)

        #Transactions
        df_pagonce19_02 = pagonce19_02
        column_name = ["ent_id", "sector_id", "subsector_id", "category_id", "category", "total_ue", "expenses_consumption", "purchase_cash_value", "purchase_cash_pct", "purchase_credit_card_value", "purchase_credit_card_pct", "purchase_bank_value", "purchase_bank_pct", "purchase_transfer_value", "purchase_transfer_pct", "purchase_check_value", "purchase_check_pct", "purchase_others_value", "purchase_others_pct", "supply_revenue", "sales_cash_value", "sales_cash_pct", "sales_credit_card_value", "sales_credit_card_pct", "sales_bank_value", "sales_bank_pct", "sales_transfer_value", "sales_transfer_pct", "sales_check_value", "sales_check_pct", "sales_others_value", "sales_others_pct", "ind1", "ind2", "ind3", "ind4", "ind5", "ind6"]

        rows_drop = ["Todos", "Sector", "Subsector"]
        df_pagonce19_02_A = general_format(df_pagonce19_02, 7, 17, column_name, "Subsector", rows_drop)   
        df_pagonce19_02_A = categories_format(df_pagonce19_02_A, column_name)

        #Transactions by state
        selected_columns = ["Column1", "Column2", "Column3", "Column5", "Column7", "Column20"]
        df_pagonce19_02_B = geo_data(df_pagonce19_02, selected_columns, 7, 17)

        df_pagonce19_02_B = df_pagonce19_02_B.melt(id_vars=fix_names, var_name="indicator", value_name="value")
        df_pagonce19_02_B["percentage"] = 0
        df_pagonce19_02_B["indicator"] = df_pagonce19_02_B["indicator"].replace({"value": "expenses_consumption_by_state", "percentage": "supply_revenue_by_state"})

        frames_business3 = [df_pagonce19_02_A, df_pagonce19_02_B]
        df_business3 = pd.concat(frames_business3)

        #Final df business
        frames_final_business = [df_business1, df_business2, df_business3]
        df_business = pd.concat(frames_final_business)


        # *********************************** STAFF *******************************************
        #Turnover and remained staff by size
        df_capance19_02 = capance19_02

        column_name_A = ["ent_id", "sector_id", "subsector_id", "category_id", "category", "total_ue", "no_training_value", "no_training_pct", "training_value", "training_pct", "staff", "temp1", "temp2", "temp3", "temp4", "value_staff_remained", "pct_staff_remained", "value_staff_turnover", "pct_staff_turnover", "temp5", "temp6", "temp7"]
        rows_drop = ["Todos", "Sector", "Subsector"]
        df_staff1 = general_format(df_capance19_02, 7, 15, column_name_A, "Subsector", rows_drop)    
        df_staff1 = yes_no_format(df_staff1, "staff_remained", "staff_turnover") 
        df_staff1["indicator"] = df_staff1["indicator"] + "_size"

        #Training
        df_edadnce19_02 = edadnce19_02

        column_name_C = ["ent_id", "sector_id", "subsector_id", "category_id", "category", "total_ue", "total_staff", "staff_remained_value", "staff_remained_pct", "staff_turnover_value", "staff_turnover_pct", "Hasta 20 años", "De 21 a 30 años", "De 31 a 40 años", "41 años o mayores", "No especificó edad", "Sin instrucción", "Educación básica", "Educación media superior", "Educación superior", "No especificó estudios"] 
        rows_drop = ["Todos", "Sector", "Subsector"]
        df_edadnce19_02 = general_format(df_edadnce19_02, 7, 14, column_name_C, "Subsector", rows_drop)  
        df_edadnce19_02 = df_edadnce19_02[["nation_id", "ent_id", "sector_id", "subsector_id", "category", "total_staff", "Hasta 20 años", "De 21 a 30 años", "De 31 a 40 años", "41 años o mayores", "No especificó edad", "Sin instrucción", "Educación básica", "Educación media superior", "Educación superior", "No especificó estudios"]]

        training = {
                        "Sí capacitaron al personal ocupado": 9,
                        "No capacitaron al personal ocupado": 10,
                    }

        df_edadnce19_02["category"] = df_edadnce19_02["category"].map(training)

        df_edadnce19_02_A = df_edadnce19_02[["nation_id", "ent_id", "sector_id", "subsector_id", "category", "total_staff", "Hasta 20 años", "De 21 a 30 años", "De 31 a 40 años", "41 años o mayores", "No especificó edad"]]
        df_edadnce19_02_A = df_edadnce19_02_A.rename(columns={"category": "indicator"})
        df_edadnce19_02_A = df_edadnce19_02_A.melt(id_vars=["nation_id", "ent_id", "sector_id", "subsector_id", "indicator", "total_staff"], var_name="category", value_name="value")
        df_edadnce19_02_A["percentage"] = (df_edadnce19_02_A["value"] / df_edadnce19_02_A["total_staff"]) * 100
        df_edadnce19_02_A["indicator"] = df_edadnce19_02_A["indicator"].replace({9: "training_by_age", 10: "no_training_by_age"})

        df_edadnce19_02_B = df_edadnce19_02[["nation_id", "ent_id", "sector_id", "subsector_id", "category", "total_staff", "Sin instrucción", "Educación básica", "Educación media superior", "Educación superior", "No especificó estudios"]]
        df_edadnce19_02_B = df_edadnce19_02_B.rename(columns={"category": "indicator"})
        df_edadnce19_02_B = df_edadnce19_02_B.melt(id_vars=["nation_id", "ent_id", "sector_id", "subsector_id", "indicator", "total_staff"], var_name="category", value_name="value")
        df_edadnce19_02_B["percentage"] = (df_edadnce19_02_B["value"] / df_edadnce19_02_B["total_staff"]) * 100
        df_edadnce19_02_B["indicator"] = df_edadnce19_02_B["indicator"].replace({9: "training_by_education", 10: "no_training_by_education"})

        df_staff2 = pd.concat([df_edadnce19_02_A, df_edadnce19_02_B])
        df_staff2 = df_staff2.drop(columns=["total_staff"])

        #Turnover and remained staff by age // Turnover and remained staff by age and education in the staff
        df_edadnce19_03 = edadnce19_03

        column_name = ["ent_id", "sector_id", "subsector_id", "category_temp", "category", "ue", "total_staff", "value_staff_remained", "pct_staff_remained", "value_staff_turnover", "pct_staff_turnover", "Hasta 20 años", "De 21 a 30 años", "De 31 a 40 años", "41 años o mayores", "No especificó edad", "Sin instrucción", "Educación básica", "Educación media superior", "Educación superior", "No especificó estudios"]
        rows_drop = ["Todos", "Sector", "Subsector"]
        df_edadnce19_03 = general_format(df_edadnce19_03, 7, 14, column_name, "Subsector", rows_drop) 

        df_edadnce19_03_A = yes_no_format(df_edadnce19_03, "staff_remained", "staff_turnover")
        df_edadnce19_03_A["indicator"] = df_edadnce19_03_A["indicator"] + "_age"

        data = []
        for item in ["Edad ", "Educación "]:
            columns = ["Hasta 20 años", "De 21 a 30 años", "De 31 a 40 años", "41 años o mayores", "No especificó edad"] if (item == "Edad ") else ["Sin instrucción", "Educación básica", "Educación media superior", "Educación superior", "No especificó estudios"]
            df_temp = df_edadnce19_03[["nation_id", "ent_id", "sector_id", "subsector_id", "category", "total_staff"] + columns]
            df_temp = df_temp.melt(id_vars=["nation_id", "ent_id", "sector_id", "subsector_id", "category", "total_staff"], var_name="category_temp", value_name="value")
            df_temp["percentage"] = (df_temp["value"] / df_temp["total_staff"]) * 100
            df_temp = df_temp.rename(columns={"category": "indicator", "category_temp": "category"})
            df_temp["indicator"] = item + df_temp["indicator"]
            df_temp = df_temp.drop(columns=["total_staff"])
            data.append(df_temp)
                
        df_edadnce19_03_B = pd.concat(data, sort=False)

        frames_staff3 = [df_edadnce19_03_A, df_edadnce19_03_B]
        df_staff3 = pd.concat(frames_staff3)

        #Final df staff
        frames_final_staff = [df_staff1, df_staff2, df_staff3]
        df_staff = pd.concat(frames_final_staff)


        # *********************************** INFORMATION TECHNOLOGY *******************************************
        #Computer service and internet
        data = []
        for df in [ticsnce19_01, ticsnce19_02]:
            name = "_size" if (df.shape[0]<15000) else "_age"
            column_names = ["ent_id", "sector_id", "subsector_id", "category_id", "category", "total_ue", "value_computer_service", "pct_computer_service", "value_no_computer_service", "pct_no_computer_service", "value_internet", "pct_internet", "value_no_internet", "pct_no_internet", "ind1", "ind2", "ind3"]
            rows_drop = ["Todos", "Sector", "Subsector"]
            df_ticsnce19_01 = general_format(df, 8, 16, column_names, "Subsector", rows_drop)
            df_ticsnce19_01_A = yes_no_format(df_ticsnce19_01, "no_computer_service", "computer_service")
            df_ticsnce19_01_B = yes_no_format(df_ticsnce19_01, "no_internet", "internet")

            frames = [df_ticsnce19_01_A, df_ticsnce19_01_B]
            df_ticsnce19_01_AB = pd.concat(frames)

            df_ticsnce19_01_AB["indicator"] = df_ticsnce19_01_AB["indicator"] + name

            data.append(df_ticsnce19_01_AB)

        df_tic1 = pd.concat(data, sort=False)


        #Internet uses by size and age
        data = []
        for df in [ticsnce19_03, ticsnce19_04]:
            name = "_size" if (df.shape[0]<15000) else "_age"
            column_names = ["ent_id", "sector_id", "subsector_id", "category_id", "category", "total_ue", "value_no_internet_services", "pct_no_internet_services", "value_internet_services", "pct_internet_services", "bank_operation_value", "bank_operation_pct", "gobernment_operation_value", "gobernment_operation_pct", "search_information_value", "search_information_pct", "business_management_value", "business_management_pct"] 
            rows_drop = ["Todos", "Sector", "Subsector"]
            df_ticsnce19_03_A = general_format(df, 7, 18, column_names, "Subsector", rows_drop)
            df_ticsnce19_03_A = categories_format(df_ticsnce19_03_A, column_names)
            df_ticsnce19_03_A["indicator"] = df_ticsnce19_03_A["indicator"] + name
            data.append(df_ticsnce19_03_A)

        df_tic2 = pd.concat(data, sort=False)


        #Internet by state
        df_ticsnce19_03 = ticsnce19_03
        selected_columns = ["Column1", "Column2", "Column3", "Column5", "Column9", "Column10"]
        df_tic3 = geo_data(df_ticsnce19_03, selected_columns, 7, 18)
        df_tic3["indicator"] = "internet_uses_by_state"

        #Final df IT
        frames_final_tic = [df_tic1, df_tic2, df_tic3]
        df_tic = pd.concat(frames_final_tic)


        # *********************************** INNOVATION *******************************************
        #Innovation by geo
        df_innonce19_01 = innonce19_01
        selected_columns = ["Column1", "Column2", "Column3", "Column5", "Column8", "Column6"]
        df_innovation1 = geo_data(df_innonce19_01, selected_columns, 9, 51)

        df_innovation1 = df_innovation1.rename(columns={"percentage": "total_ue"})

        df_innovation1["percentage"] = (df_innovation1["value"] / df_innovation1["total_ue"]) * 100
        df_innovation1 = df_innovation1.drop(columns="total_ue")
        df_innovation1["indicator"] = "innovation_by_state"


        #Innovation activities in 2018
        df_innonce19_04 = innonce19_04
        column_name = ["ent_id", "sector_id", "subsector_id", "rama_id", "category", "total_ue", "value_ue_no_innovation", "value_ue_innovation", "value_total_staff", "staff_products_value", "staff_process_value", "staff_bussiness_value", "staff_organization_value", "staff_documentation_value", "ind1", "ind2", "ind3"]
        rows_drop = ["Rama"]
        df_innonce19_04 = general_format(df_innonce19_04,8,15, column_name, "Subsector", rows_drop)
        df_innonce19_04["category"] = "No aplica"

        df_innovation2 = df_innonce19_04[fix_names + ["total_ue", "value_ue_no_innovation", "value_ue_innovation"]]
        for i in ["value_ue_no_innovation", "value_ue_innovation"]:
            df_innovation2["pct_"+i] = (df_innovation2[i] / df_innovation2["total_ue"]) * 100

        df_innovation2 = df_innovation2.rename(columns={"pct_value_ue_no_innovation": "pct_ue_no_innovation", "pct_value_ue_innovation": "pct_ue_innovation"})

        df_innovation2 = yes_no_format(df_innovation2, "ue_no_innovation", "ue_innovation")

        #Staff by innovation activity in 2018
        df_innovation3 = df_innonce19_04[fix_names + ["value_total_staff", "staff_products_value", "staff_process_value", "staff_bussiness_value", "staff_organization_value", "staff_documentation_value"]]
        for i in ["staff_products_value", "staff_process_value", "staff_bussiness_value", "staff_organization_value", "staff_documentation_value"]:
            df_innovation3[i+"_pct"] = df_innovation3[i].div(df_innovation3["value_total_staff"].where(df_innovation3["value_total_staff"] != 0, np.nan))
            df_innovation3[i+"_pct"] = df_innovation3[i+"_pct"].fillna(0)
            df_innovation3[i+"_pct"] = df_innovation3[i+"_pct"]*100

        df_innovation3.columns = df_innovation3.columns.str.replace("_value_pct", "_pct")
        df_innovation3 = categories_format(df_innovation3, list(df_innovation3))


        #Innovation by year
        df_innonce19_06 = innonce19_06

        column_name = ["ent_id", "sector_id", "subsector_id", "rama_id", "category", "total_ue", "no_staff_id", "staff_id", "staff_id2016", "staff_id2017", "staff_id2018", "no_biotechnology", "biotechnology", "biotechnology2016", "biotechnology2017", "biotechnology2018", "no_nanotechnology", "nanotechnology", "nanotechnology2016", "nanotechnology2017", "nanotechnology2018", "no_patents", "patents", "patents2016", "patents2017", "patents2018", "no_patents_contract", "patents_contract", "patents_contract2016", "patents_contract2017", "patents_contract2018"]
        rows_drop = ["Rama"]
        df_innonce19_06 = general_format(df_innonce19_06,7,15, column_name, "Subsector", rows_drop)
        df_innonce19_06["category"] = "No aplica"

        df_innovation4 = df_innonce19_06[fix_names + [s for s in column_name if "201" in s]]
        df_innovation4 = df_innovation4.melt(id_vars=fix_names, var_name="indicator", value_name="value")
        df_innovation4["percentage"] = 0

        #Final df innovation
        frames_final_innovation = [df_innovation1, df_innovation2, df_innovation3, df_innovation4]
        df_innovation = pd.concat(frames_final_innovation)


        # *********************************** ENVIRONMENTAL *******************************************
        #Knowledge of environmental regulations
        df_medance19_01 = medance19_01

        df_medance19_01 = df_medance19_01.iloc[9:]
        df_medance19_01 = df_medance19_01[:-11]

        column_name = ["ent_id", "sector_id", "category", "total_ue", "meets_standard_value", "meets_standard_pct", "no_meets_standard_value", "no_meets_standard_pct", "no_know_standard_value", "no_know_standard_pct"]
        df_medance19_01.columns = column_name

        df_medance19_01 = df_medance19_01[df_medance19_01["sector_id"].notna()]

        df_medance19_01["ent_id"] = df_medance19_01["ent_id"].str[0:2].astype(int)
        df_medance19_01["nation_id"] = df_medance19_01["ent_id"].apply(lambda x: 1 if x == 0 else 0)

        df_medance19_01["sector_id"] = df_medance19_01["sector_id"].str.replace("Sector ", "")
        df_medance19_01 = df_medance19_01.assign(sector_id=lambda d: d["sector_id"].str.strip().str.replace(" +", " "))

        df_medance19_01_A = df_medance19_01[["nation_id", "ent_id", "sector_id"] + [s for s in column_name if "_value" in s]]
        df_medance19_01_A = df_medance19_01_A.melt(id_vars=["nation_id", "ent_id", "sector_id"], var_name="category", value_name="value")

        df_medance19_01_B = df_medance19_01[["nation_id", "ent_id", "sector_id"] + [s for s in column_name if "_pct" in s]]
        df_medance19_01_B.columns = df_medance19_01_B.columns.str.replace("_pct", "_value")
        df_medance19_01_B = df_medance19_01_B.melt(id_vars=["nation_id", "ent_id", "sector_id"], var_name="category", value_name="percentage")

        df_enviromental1 = pd.merge(df_medance19_01_A, df_medance19_01_B, on=["nation_id", "ent_id", "sector_id", "category"])

        df_enviromental1["subsector_id"] = 0
        df_enviromental1["indicator"] = "environmental_regulations"


        #Staff engaged in environmental protection activities
        df_medance19_05 = medance19_05

        df_medance19_05 = df_medance19_05.iloc[9:]
        df_medance19_05 = df_medance19_05[:-13]

        column_name = ["ent_id", "sector_id", "category", "total_ue", "no_staff_environmental_value", "no_staff_environmental_pct", "value_staff_environmental_ue", "pct_staff_environmental_ue", "total_staff", "value_staff_environmental", "pct_staff_environmental", "worked_hours"]
        df_medance19_05.columns = column_name

        df_medance19_05 = df_medance19_05[df_medance19_05["sector_id"].notna()]

        df_medance19_05["ent_id"] = df_medance19_05["ent_id"].str[0:2].astype(int)
        df_medance19_05["nation_id"] = df_medance19_05["ent_id"].apply(lambda x: 1 if x == 0 else 0)

        df_medance19_05["sector_id"] = df_medance19_05["sector_id"].str.replace("Sector ", "")
        df_medance19_05 = df_medance19_05.assign(sector_id=lambda d: d["sector_id"].str.strip().str.replace(" +", " "))
        df_medance19_05["subsector_id"] = 0
        df_medance19_05["category"] = "No aplica"

        df_enviromental2 = yes_no_format(df_medance19_05, "staff_environmental_ue", "staff_environmental")


        #Waste separation
        df_medance19_03 = medance19_03
        column_name = ["ent_id", "sector_id", "category", "total_ue", "value_no_separate_waste", "pct_no_separate_waste", "value_separate_waste", "pct_separate_waste", "paper_value", "paper_pct", "textile_value", "textile_pct", "wood_value", "wood_pct", "metal_value", "metal_pct", "glass_value", "glass_pct", "plastic_value", "plastic_pct", "organic_value", "organic_pct", "other_waste_value", "other_waste_pct"]
        df_medance19_03 = environmental(df_medance19_03, 9, 13, column_name)

        df_medance19_03_A = yes_no_format(df_medance19_03, "no_separate_waste", "separate_waste")
        df_medance19_03_B = categories_format(df_medance19_03, column_name)
        df_medance19_03_B = df_medance19_03_B.drop(columns="category")
        df_medance19_03_B = df_medance19_03_B.rename(columns={"indicator": "category"})
        df_medance19_03_B["indicator"] = "waste_separation"

        df_enviromental3 = pd.concat([df_medance19_03_A, df_medance19_03_B])


        #Expenses, Investment and uses of water
        data = []
        for df in [medance19_07, medance19_08, medance19_09]:
            cut = 15 if (df.shape[1]==23) else 11
            name_dict = {"Gasto en protección al medio ambiente3": "environmental_expenses", "Inversión en protección al medio ambiente3": "environmental_investment", "Uso principal del agua tratada": "process_water"}
            name = name_dict[df.iloc[3,8]]
            column_name78 = ["ent_id", "sector_id", "category", "total_ue", "value_no_"+name, "pct_no_"+name, "value_"+name, "pct_"+name, "total_"+name, "energy_value", "water_value", "process_value", "transport_no_dangerous_value", "transport_dangerous_value", "sewer_value", "ecosystem_value", "noise_value", "sewage_water_value", "vehicle_value", "atmosphere_value", "campaigns_value", "external_services_value", "other_expenses_value"]
            column_name9 = ["ent_id", "sector_id", "category", "total_process_water", "value_no_process_water", "pct_no_process_water", "value_process_water", "pct_process_water", "production_value", "cooling_value", "garden_value", "discharge_value", "other_water_uses_value"]
            column_names = column_name78 if (df.shape[1]==23) else column_name9
            
            df_medance19_07 = environmental(df, 9, cut, column_names)
            

            df_medance19_07_A = yes_no_format(df_medance19_07, "no_"+name, name)

            df_medance19_07_B = df_medance19_07[fix_names + ["total_"+name] + [s for s in column_names if "_value" in s]]
            for i in [s for s in column_names if "_value" in s]:
                df_medance19_07_B[i+"_pct"] = (df_medance19_07_B[i] / df_medance19_07_B["total_"+name]) * 100

            df_medance19_07_B.columns = df_medance19_07_B.columns.str.replace("_value_", "_")
            df_medance19_07_B = categories_format(df_medance19_07_B, list(df_medance19_07_B))
            df_medance19_07_B = df_medance19_07_B.drop(columns="category")
            df_medance19_07_B = df_medance19_07_B.rename(columns={"indicator": "category"})
            df_medance19_07_B["indicator"] = "types_"+name

            df_medance19_07 = pd.concat([df_medance19_07_A, df_medance19_07_B])
            data.append(df_medance19_07)
            
        df_enviromental4 = pd.concat(data, sort=False)


        #Final df environmental
        frames_final_environmental = [df_enviromental1, df_enviromental2, df_enviromental3, df_enviromental4]
        df_environmental = pd.concat(frames_final_environmental)


        # *********************************** FINAL DF *******************************************
        #Concat all dataframes
        frames_final = [df_financing, df_internet, df_business, df_staff, df_tic, df_innovation, df_environmental]
        df = pd.concat(frames_final)

        df["category"] = df["category"].replace(dicto_categories)
        df["indicator"] = df["indicator"].replace(dicto_indicators)

        df = df.dropna(how="all", subset=["value", "percentage"])

        for i in ["value", "percentage"]: 
            df[i] = df[i].astype(float)
  
        for i in ["subsector_id", "category", "indicator"]:
          df[i] = df[i].astype(int)
        
        df['sector_id'] = df['sector_id'].astype(str)

        df['nation_id'].replace({1: 'mex'}, inplace=True)
        df['nation_id'] = df['nation_id'].astype(str)

        df.reset_index(drop=True, inplace=True)

        return df

class IndicatorsCensusPipeline(EasyPipeline):
    @staticmethod
    def steps(params):
        db_connector = Connector.fetch(
            "clickhouse-database", open("../conns.yaml"))

        dtypes = {
            "nation_id":        "String",
            "ent_id":           "UInt8",
            "sector_id":        "String",
            "subsector_id":     "UInt16",
            "indicator":        "UInt8",
            "category":         "UInt8",
            "value":            "UInt32",
            "percentage":       "Float32"
        }

        download_step = DownloadStep(
            connector=["dim_categories", "dim_indicators", "capance19_02", "contnce19_02", "crednce19_03", "crednce19_04", "crednce19_05", "crednce19_06",
                        "crednce19_07", "crednce19_08", "ecomnce19_03", "ecomnce19_04", "ecomnce19_05", "ecomnce19_06", "edadnce19_02", "edadnce19_03",
                        "innonce19_01", "innonce19_04", "innonce19_06", "medance19_01", "medance19_03", "medance19_05", "medance19_07", "medance19_08",
                        "medance19_09", "pagonce19_02", "probnce19_03", "ticsnce19_01", "ticsnce19_02", "ticsnce19_03", "ticsnce19_04"],
            connector_path="conns.yaml"
        )

        transform_step = TransformStep()
        
        load_step = LoadStep(
            "indicators_economic_census_2019", db_connector, if_exists="drop",
            pk=["nation_id", "ent_id", "sector_id", "subsector_id", "indicator", "category"], dtype=dtypes, nullable_list=["value"]
        )

        return [download_step, transform_step, load_step]

if __name__ == "__main__":
    indicators_economic_census_2019 = IndicatorsCensusPipeline()
    indicators_economic_census_2019.run({})