import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import LoadStep


class ExtractStep(PipelineStep):
    def run_step(self, prev, params):
        data = {'id':[1, 2, 3, 4, 8, 9], 
                'name_en':['Has no difficulty', 'Does it with little difficulty', 'Does it with great difficulty', 'Can not do it', 'Unknown degree of disability', 'Not Specified'],
                'name_es':['No tiene dificultad', 'Lo hace con poca dificultad', 'Lo hace con mucha dificultad', 'No puede hacerlo', 'Desconoce el grado de discapacidad', 'No Especificado']}
        df = pd.DataFrame(data=data)

        return df


class DimImperimentPipeline(EasyPipeline):
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
            "dim_inegi_imperiment", db_connector, if_exists="drop", dtype=dtype,
            pk=['id']
        )

        return [extract_step, load_step]