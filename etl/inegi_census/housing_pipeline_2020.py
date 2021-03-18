import pandas as pd
from bamboo_lib.models import Parameter, EasyPipeline, PipelineStep
from bamboo_lib.connectors.models import Connector
from bamboo_lib.steps import LoadStep, DownloadStep

class ReadStep(PipelineStep):
    def run_step(self, prev, params):
        df = pd.read_csv(prev)
        df.columns = df.columns.str.lower()
        return df

class CleanStep(PipelineStep):
    def run_step(self, prev, params):
        df = prev
        
        labels = ['clavivp', 'forma_adqui', 'paredes', 'techos', 'pisos', 'cuadorm', 
                'totcuart', 'numpers', 'ingtrhog', 'refrigerador', 'lavadora', 
                'autoprop', 'televisor', 'internet', 'computadora', 'celular']

        extra_labels = ['bomba_agua', 'calentador_solar', 'aire_acon', 'panel_solar', 
        'separacion1', 'horno', 'motocicleta', 'bicicleta', 'serv_tv_paga', 'serv_pel_paga', 
        'con_vjuegos', 'escrituras', 'deuda']      

        df = df[['ent', 'mun', 'loc50k', 'factor'] + labels + extra_labels].copy()

        dtypes = {
            'ent': 'str',
            'mun': 'str',
            'loc50k': 'str',
            'clavivp': 'int',
            'forma_adqui': 'int',
            'paredes': 'int',
            'techos': 'int',
            'pisos': 'int',
            'cuadorm': 'int',
            'totcuart': 'int',
            'numpers': 'int',
            'ingtrhog': 'float',
            'factor': 'int',
            'refrigerador': 'int',
            'lavadora': 'int', 
            'autoprop': 'int', 
            'televisor': 'int', 
            'internet': 'int', 
            'computadora': 'int', 
            'celular': 'int',
            'bomba_agua': 'int',
            'calentador_solar': 'int',
            'aire_acon': 'int',
            'panel_solar': 'int',
            'separacion1': 'int',
            'horno': 'int',
            'motocicleta': 'int',
            'bicicleta': 'int',
            'serv_tv_paga': 'int',
            'serv_pel_paga': 'int',
            'con_vjuegos': 'int',
            'escrituras': 'int',
            'deuda': 'int'
        }

        for key, val in dtypes.items():
            try:
                df.loc[:, key] = df[key].astype(val)
                continue
            except:
                df.loc[:, key] = df[key].astype('float')

        return df, labels, extra_labels

class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        df = prev[0]

        print(df)
        print(df.columns)
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

        download_step = DownloadStep(
            connector="housing-data-2020",
            connector_path="conns.yaml"
        )

        read_step = ReadStep()
        clean_step = CleanStep()
        transform_step = TransformStep()
        
        return [download_step, read_step, clean_step, transform_step]

if __name__ == "__main__":
    pp = HousingPipeline()
    for index in range(1, 1 + 1):
        pp.run({
            "index": str(index).zfill(2)
            })