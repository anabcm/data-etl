import pandas as pd
from bamboo_lib.models import PipelineStep
from bamboo_lib.models import Parameter, EasyPipeline
from bamboo_lib.connectors.models import Connector
from bamboo_lib.steps import LoadStep, DownloadStep

class ReadStep(PipelineStep):
    def run(prev, self, params):
        df = pd.read_csv(prev)
        df.columns = df.columns.str.lower()
        return df

class TransformStep(PipelineStep):
    def run_step(self, prev, params):

        return df

class HousingPipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return [
            Parameter(label="Index", name="index", dtype=str)
        ]

    @staticmethod
    def steps(params, **kwargs):
        # Use of connectors specified in the conns.yaml file
        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))

        dtype = {}

        dl_step = DownloadStep(
            connector='housing-data',
            connector_path='conns.yaml'
        )

        read_step = ReadStep()
        transform_step = TransformStep()
        load_step = LoadStep()
        
        return [dl_step, read_step]

if __name__ = __"main"__:
    pp = HousingPipeline()
    pp.run({"index": index})
        