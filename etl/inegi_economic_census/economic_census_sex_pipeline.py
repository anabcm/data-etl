import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import DownloadStep
from bamboo_lib.steps import LoadStep


class TransformStep(PipelineStep):
    def run_step(self, prev, params):

        df = pd.read_csv(prev, index_col=None, encoding="latin-1", low_memory=False)

        ren_cols = {
            "H001B": "employed_workers",
            "H000B": "personal_dependent_on_the_social_reason",
            "H010B": "paid_staff",
            "H101B": "production_personnel_sales_and_service",
            "H203B": "administrative_accounting_and_managerial_staff",
            "H020B": "owners_family_and_other_unpaid_workers",
            "I000B": "staff_not_dependent_on_the_social_reason",
            "I100B": "personnel_provided_by_another_company_name",
            "I200B": "staff_for_fees_or_commissions_without_basic_salary",
            "H001C": "employed_workers",
            "H000C": "personal_dependent_on_the_social_reason",
            "H010C": "paid_staff",
            "H101C": "production_personnel_sales_and_service",
            "H203C": "administrative_accounting_and_managerial_staff",
            "H020C": "owners_family_and_other_unpaid_workers",
            "I000C": "staff_not_dependent_on_the_social_reason",
            "I100C": "personnel_provided_by_another_company_name",
            "I200C": "staff_for_fees_or_commissions_without_basic_salary"
        }

        # Deleting no usable rows
        df.drop_duplicates(subset=["Año Censal", "Entidad", "Municipio", "Actividad Económica", "UE Unidades económicas"], inplace=True)
        df.drop(["Unnamed: 118"], axis=1, inplace=True)
        df.drop(df[df["Entidad"]=="00 Total Nacional"].index, inplace=True)

        # Renaming columns
        temp = list(df.columns[:4])
        for i in range(4, len(df.columns)):
            temp.append(df.columns[i].split(" ")[0])
        df.columns = temp

        # Deleting duplicated columns, and empty rows
        df = df.loc[:, ~df.columns.duplicated()]
        df.drop(df.columns[[-1,-2,-3]], axis=1, inplace=True)

        # Drops "Total" rows and empty columns
        df.drop(df.loc[df["Actividad Económica"].str.match('Total nacional') | df["Actividad Económica"].str.match('Total municipal')].index, inplace=True)
        df=df[pd.notnull(df["Año Censal"])]
        df["Año Censal"]=df["Año Censal"].astype(int)

        # Creating mun_id code
        piv_ent = df["Entidad"].str.split(" ", n=1, expand=True)
        piv_mun = df["Municipio"].str.split(" ", n=1, expand=True)
        df["Entidad"] = piv_ent[0] + piv_mun[0]
        df.drop(["Municipio"], axis=1, inplace=True)

        # Renaming columns
        df.rename(index=str, columns={"Año Censal": "year", "Entidad": "mun_id", "Actividad Económica": "national_industry_id"}, inplace=True)

        # Getting national industry id
        piv_class = df["national_industry_id"].str.split(" ", n=1, expand=True)
        df["national_industry_id"] = piv_class[0]

        # Useless 3 columns
        df.drop(df.columns[[-1,-2,-3]], axis=1, inplace=True)

        # Gender lists
        males_  = ["H001B", "H000B", "H010B", "H101B", "H203B", "H020B", "I000B", "I100B", "I200B"]
        females_= ["H001C", "H000C", "H010C", "H101C", "H203C", "H020C", "I000C", "I100C", "I200C"]

        # Creating gender based dataframes
        df_males = df[["year", "mun_id", "national_industry_id"] + males_].copy()
        df_females = df[["year", "mun_id", "national_industry_id"] + females_].copy()

        # Setting sex id's
        df_males["sex"] = 1
        df_females["sex"] = 2

        # Renaming columns from the male and female dataframes, pre concat
        df_males.rename(index=str, columns=dict(ren_cols), inplace=True)
        df_females.rename(index=str, columns=dict(ren_cols), inplace=True)

        # Concat step to unite male and female dataframes
        df = pd.concat([df_males, df_females], axis=0, join='outer', ignore_index=False, keys=None,
                levels=None, names=None, verify_integrity=False, copy=True)

        # Groupby step
        df = df.groupby(["year", "mun_id", "national_industry_id", "sex", "employed_workers", "personal_dependent_on_the_social_reason",
        "paid_staff", "production_personnel_sales_and_service", "administrative_accounting_and_managerial_staff",
        "owners_family_and_other_unpaid_workers", "staff_not_dependent_on_the_social_reason", "personnel_provided_by_another_company_name",
        "staff_for_fees_or_commissions_without_basic_salary"]).sum().reset_index(col_fill="ffill")

        # Setting types
        for item in ["year", "mun_id", "sex", "employed_workers", "personal_dependent_on_the_social_reason", "paid_staff",
        "production_personnel_sales_and_service", "administrative_accounting_and_managerial_staff",
        "owners_family_and_other_unpaid_workers", "staff_not_dependent_on_the_social_reason", "personnel_provided_by_another_company_name",
        "staff_for_fees_or_commissions_without_basic_salary"]:
            df[item] = df[item].astype(int)

        return df

class EconomicCensusGenderPipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return []

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch("clickhouse-database", open("../conns.yaml"))

        dtype = {
            "mun_id":                                                   "UInt16",
            "year":                                                     "UInt16",
            "sex":                                                      "UInt8",
            "national_industry_id":                                     "String",
            "employed_workers":                                         "UInt32",
            "personal_dependent_on_the_social_reason":                  "UInt32",
            "paid_staff":                                               "UInt32",
            "production_personnel_sales_and_service":                   "UInt32",
            "administrative_accounting_and_managerial_staff":           "UInt16",
            "owners_family_and_other_unpaid_workers":                   "UInt16",
            "staff_not_dependent_on_the_social_reason":                 "UInt16",
            "personnel_provided_by_another_company_name":               "UInt16",
            "staff_for_fees_or_commissions_without_basic_salary":       "UInt16"
        }

        download_step = DownloadStep(
            connector="dataset",
            connector_path="conns.yaml"
        )
        
        transform_step = TransformStep()
        load_step = LoadStep(
            "inegi_economic_census_sex", db_connector, if_exists="append", pk=["mun_id", "year"], dtype=dtype, 
        )

        return [download_step, transform_step, load_step]