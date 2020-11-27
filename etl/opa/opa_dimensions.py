
import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, Parameter, PipelineStep
from bamboo_lib.steps import DownloadStep, LoadStep

class DimRamosStep(PipelineStep):
    def run_step(self, prev, params):

        df = pd.read_excel(prev)

        # columns = ["id_investment_area", "investment_area_es", "investment_area_en"]

        return df

class DimURStep(PipelineStep):
    def run_step(self, prev, params):

        df = pd.read_csv(prev)

        # columns = ["id_responsible_unit", "responsible_unit_es", "responsible_unit_en"]
        # 'id_responsible_unit' = HHE does not have 'responsible_unit' -> No especificado

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

        if params.get('pipeline') == 'ramos':

            download_step = DownloadStep(
                connector='opa-investment-area',
                connector_path="conns.yaml"
            )

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

            download_step = DownloadStep(
                connector='opa-responsible-unit',
                connector_path="conns.yaml"
            )

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