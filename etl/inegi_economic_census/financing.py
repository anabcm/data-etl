import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, PipelineStep, Parameter
from bamboo_lib.steps import DownloadStep, LoadStep


#class ReadStep(PipelineStep):
#    def run_step(self, prev, params):
#        # read data
#        data3, data4, data5, data6 = prev
#        df3 = pd.read_excel(prev[0])
#        df4 = pd.read_excel(prev[1])
#        df5 = pd.read_excel(prev[2])
#        df6 = pd.read_excel(prev[3])

#       return df3, df4, df5, df6

class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        df3 = pd.read_excel(prev[0])
        df4 = pd.read_excel(prev[1])
        df5 = pd.read_excel(prev[2])
        df6 = pd.read_excel(prev[3])
        

        #Dictionaries company age and company size
        company_size = {
                "Hasta    10 personas": 1,
                "11 a    50 personas": 2,
                "51 a  250 personas": 3,
                "251 y más personas": 4
            }

        company_age = {
                        "De reciente creación (hasta  2 años)": 1,
                        "Jóvenes (3 a 5 años)": 2,
                        "Adultas (6 a 10 años)": 3,
                        "Mayores (más de 10 años)": 4
                    }

        data = []
        for df_ in [df3, df4, df5, df6]:
            name = "size" if df_.shape[0]<15000 else "age" #df3 o df5 --> size
            dict_values = company_size if df_.shape[0]<15000 else company_age
            
            if df_.shape[1]==24:

                df = df_.iloc[8:]
                df = df[:-15]
                
                df.columns = ["ent_id", "sector_id", "subsector_id", name, "general_name", "total_ue", 
                                "ue_with_financing", "pct_with_financing", "Banks", "pct_bank", 
                                "Popular Savings Banks", "pct_saving", "Providers", "pct_providers", "Relatives or Friends", "pct_friends", 
                                "Goverment", "pct_goverment", "Loans", "pct_loans", "Partners", "pct_partners",
                                "Others", "pct_others"] 
                
                df["ent_id"] = df["ent_id"].str[0:2].astype(int)
                df = df.drop(df[df["ent_id"] == 0].index)
                df = df.dropna(subset=["sector_id", "subsector_id", name])

                df["subsector_id"] = df["subsector_id"].str.replace("Subsector ", "")

                df = df.loc[:,~df.columns.str.startswith("pct")]

                df["general_name"] = df["general_name"].str.strip()
            
                df[name+"_id"] = df["general_name"].map(dict_values)

                df["Others (government, private, partners)"] = df["Goverment"] + df["Loans"] + df["Partners"] + df["Others"]

                df = df.drop(columns=["sector_id", "general_name", name, "Goverment", "Loans", "Partners", "Others"])

                df = df.melt(id_vars=["ent_id", "subsector_id", "total_ue", "ue_with_financing", name+"_id"], var_name="funding_source", value_name="financed_ue")

                for i in ["subsector_id", "total_ue", "ue_with_financing", "financed_ue"]:
                    df[i] =  df[i].astype(int)
            
            else:
   
                df = df_.iloc[9:]
                df = df[:-15]

                df.columns = ["ent_id", "sector_id", "subsector_id", name, "general_name", "total_ue", 
                                "ue_with_financing", "pct_with_financing", "Business Creation", "pct_business", 
                                "Equipment or business expansion", "pct_equipment", "Purchase of premises or vehicle", 
                                "pct_premises", "Debt payment", "pct_debt", "Acquisition of Inputs in the National market", 
                                "pct_inputs_nat", "Acquisition of Inputs in the Foreign market", "pct_inputs_foreign", 
                                "Payment of wages", "pct_wages", "Did not specify", "pct_no_specify", "ind1", "ind2"]

                df["ent_id"] = df["ent_id"].str[0:2].astype(int)
                df = df.drop(df[df["ent_id"] == 0].index)
                df = df.dropna(subset=["sector_id", "subsector_id", name])

                df["subsector_id"] = df["subsector_id"].str.replace("Subsector ", "")
                df = df.loc[:,~df.columns.str.startswith("pct")]

                df["general_name"] = df["general_name"].str.strip()

                df[name+"_id"] = df["general_name"].map(dict_values)

                df = df.drop(columns=["sector_id", "general_name", name, "ind1", "ind2"])

                df = df.melt(id_vars=["ent_id", "subsector_id", "total_ue", "ue_with_financing", name+"_id"], var_name="uses_of_financing", value_name="ue_by_uses_of_financing")

                for i in ["subsector_id", "total_ue", "ue_with_financing", "ue_by_uses_of_financing"]:
                    df[i] =  df[i].astype(int)

            data.append(df)

        df_final = pd.concat(data, sort=False)
    
        for i in ["funding_source", "uses_of_financing"]:
            df_final[i] = df_final[i].fillna("Does not apply")
            
        df_final = df_final.fillna(0)
        for i in ["financed_ue",  "size_id", "ue_by_uses_of_financing", "age_id"]:
           
            df_final[i] = df_final[i].astype(int)
                
        df_final["filter_source_financing"] = [1 if x != "Does not apply" else 0 for x in df_final["funding_source"]]

        df_final["dataset_size"] = [1 if x != 0 else 0 for x in df_final["size_id"]]
  
        return df_final

class FinancingCensusPipeline(EasyPipeline):

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch(
            "clickhouse-database", open("../conns.yaml"))

        dtypes = {
            "ent_id":                    "UInt8",
            "subsector_id":              "UInt16",
            "total_ue":                  "UInt32",
            "ue_with_financing":         "UInt16",
            "size_id":                   "UInt8",
            "funding_source":            "String",
            "financed_ue":               "UInt16",
            "age_id":                    "UInt8",
            "uses_of_financing":         "String",
            "ue_by_uses_of_financing":   "UInt16",
            "filter_source_financing":   "UInt8",
            "dataset_size":              "UInt8"
        }

        download_step = DownloadStep(
            connector=["data-financing3", "data-financing4", "data-financing5", "data-financing6"],
            connector_path="conns.yaml"
        )

       # read_step = ReadStep()
        transform_step = TransformStep()
        
        load_step = LoadStep(
            "financing_census", db_connector, if_exists="append",
            pk=["ent_id", "subsector_id", "total_ue", "ue_with_financing", "size_id", "financed_ue", 
                "age_id", "ue_by_uses_of_financing", "filter_source_financing", "dataset_size"], dtype=dtypes
        )

        return [download_step, transform_step, load_step]

if __name__ == "__main__":
    financing_census_pipeline = FinancingCensusPipeline()
    financing_census_pipeline.run({})