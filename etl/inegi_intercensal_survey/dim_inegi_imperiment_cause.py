import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import LoadStep


class ExtractStep(PipelineStep):
    def run_step(self, prev, params):
        data = {'id':[1, 2, 3, 4, 5, 9], 
                'name_en':['Because he was born that way', 'Because of a disease', 'Because of an accident', 'Because of advanced age', 'Because of another cause', 'Not Specified'],
                'name_es':['Porque nació asi', 'Por una enfermedad', 'Por un accidente', 'Por edad avanzada', 'Por otra causa', 'No Especificado']}
        df = pd.DataFrame(data=data)

        return df


class DimImperimentCausePipeline(EasyPipeline):
    @staticmethod
    def steps(params):
        db_connector = Connector.fetch('clickhouse-database', open("../conns.yaml"))

        dtype = {
            'id':       'UInt8',
            'name_es':  'String',
            'name_en':  'String',
        }

        extract_step = ExtractStep()
        load_step = LoadStep(
            "dim_inegi_imperiment_cause", db_connector, if_exists="drop", dtype=dtype,
            pk=['id']
        )

        return [extract_step, load_step]