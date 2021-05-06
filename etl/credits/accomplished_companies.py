import numpy as np
import pandas as pd

from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import DownloadStep
from bamboo_lib.steps import LoadStep

from shared import AGE_RANGE, PERSON_TYPE, SEX

columns_to_rename = {
    "clave_municipal":"mun_id",
    "conteo_anonimizado":"count",
    "sexo":"sex",
    "tipo_persona":"person_type",
    "rango_edad_antigüedad":"age_range"
}

class XformStep(PipelineStep):
    def run_step(self, prev, params):
        df = pd.read_excel(prev)
        
        #Clean columns names
        df.columns = df.columns.str.lower()
        df.columns = df.columns.str.replace(' ','_')
        df.columns = df.columns.str.replace('.','_')

        #Rename columns
        df.rename(index=str, columns=columns_to_rename, inplace=True)

        #Replace members
        df['sex'].replace(SEX, inplace=True)
        df['person_type'].replace(PERSON_TYPE, inplace=True)
        df['age_range'].replace(AGE_RANGE, inplace=True)

        #Drop no need columns
        df.drop(columns=['nom_ent', 'nom_mun'], inplace=True)

        return df

class AccomplishedCompaniesPipeline(EasyPipeline):
    @staticmethod
    def parameters_list():
        return

    @staticmethod    
    def steps(params):
        db_connector = Connector.fetch("clickhouse-database", open("../conns.yaml"))
        
        dtype = {
            "mun_id":           "UInt16",
            "sex":              "UInt8",
            "person_type":      "UInt8",
            "age_range":        "UInt8",
            "count": "UInt8"
        }

        dl_step = DownloadStep(
            connector="accomplished-companies-mun-total",
            connector_path="conns.yaml"
        )

        xform_step = XformStep()

        ld_step = LoadStep(
            'accomplished_companies_credits', 
            db_connector, 
            if_exists="drop",
            pk=["mun_id", "sex", "age_range"],
            dtype=dtype
            )

        return [dl_step, xform_step, ld_step]