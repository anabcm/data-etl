
import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, Parameter, PipelineStep
from bamboo_lib.steps import DownloadStep, LoadStep

class DimRamosStep(PipelineStep):
    def run_step(self, prev, params):

        df = pd.read_csv(prev)

        df.columns = df.columns.str.lower()

        df = df[['id_ramo', 'descripcion_ramo']].drop_duplicates(keep='last').copy()
        df.columns = ['id_investment_area', 'investment_area']

        return df

class DimURStep(PipelineStep):
    def run_step(self, prev, params):

        df = pd.read_csv(prev)

        df.columns = df.columns.str.lower()
        df = df[['id_ur', 'descripcion_ur']].drop_duplicates().copy()
        df.columns = ['id_responsible_unit', 'responsible_unit']
        # 'id_responsible_unit' = HHE does not have 'responsible_unit'
        df.dropna(inplace=True)

        return df

class OPADimsMobility(EasyPipeline):
    @staticmethod
    def parameter_list():
        return [
            Parameter(label="pipeline", name="pipeline", dtype=str)
        ]

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch("clickhouse-database", open("../conns.yaml"))

        download_step = DownloadStep(
            connector='opa-data',
            connector_path="conns.yaml"
        )

        if params.get('pipeline') == 'ramos':

            transform_step = DimRamosStep()

            load_step = LoadStep(
                "dim_opa_investment_area", db_connector, if_exists="drop", pk=["id_investment_area"], 
                dtype = {
                    "id_investment_area": "UInt8",
                    "investment_area":    "String"
                }
            )

            return [download_step, transform_step, load_step]
        
        else:

            transform_step = DimURStep()

            load_step = LoadStep(
                "dim_opa_ur", db_connector, if_exists="drop", pk=["id_responsible_unit"],
                dtype = {
                    "id_responsible_unit": "String",
                    "responsible_unit":    "String"
                }
            )

            return [download_step, transform_step, load_step]

if __name__ == "__main__":
    pp = OPADimsMobility()
    for dim in ['ramos', 'ur']:
        pp.run({
            'pipeline': dim
        })