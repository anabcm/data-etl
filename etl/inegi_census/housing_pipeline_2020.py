import pandas as pd
from bamboo_lib.models import Parameter, EasyPipeline, PipelineStep
from bamboo_lib.connectors.models import Connector
from bamboo_lib.steps import LoadStep, DownloadStep

class ReadStep(PipelineStep):
    def run_step(self, prev, params):
        df = pd.read_csv(prev[0])
        df.columns = df.columns.str.lower()
        return df, prev[1]

class CleanStep(PipelineStep):
    def run_step(self, prev, params):
        df, dimension = prev
        
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

        return df, labels, extra_labels, dimension

class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        df, labels, extra_labels, dimension = prev

        # data to replace
        data = {}
        for col in ['clavivp', 'paredes', 'techos', 'pisos', 'cuadorm', 'totcuart', 'refrigerador', 'lavadora', 'autoprop', 'televisor', 'internet', 'computadora', 'celular', 'bomba_agua', 'calentador_solar', 'aire_acon', 'panel_solar', 'separacion1', 'horno', 'motocicleta', 'bicicleta', 'serv_tv_paga', 'serv_pel_paga', 'con_vjuegos', 'escrituras', 'deuda']:
            data[col] = pd.read_excel(dimension, sheet_name=col, dtype='object')

        # location id
        df['loc_id'] = (df.ent.astype('str') + df.mun.astype('str') + df.loc50k.astype('str')).astype('int')
        df.drop(columns=['ent', 'mun', 'loc50k'], inplace=True)
        df.ingtrhog = df.ingtrhog.fillna(-5).round(0).astype('int64')

        # income interval replace
        income = pd.read_excel(dimension, sheet_name='income')
        for ing in df.ingtrhog.unique():
            for level in range(income.shape[0]):
                if (ing >= income.interval_lower[level]) & (ing < income.interval_upper[level]):
                    df.ingtrhog = df.ingtrhog.replace(ing, str(income.id[level]))
                    break
                if ing >= income.interval_upper[income.shape[0]-1]:
                    df.ingtrhog = df.ingtrhog.replace(ing, str(income.id[income.shape[0]-1]))
                    break
        df.ingtrhog = df.ingtrhog.astype('int')
        df.ingtrhog.replace(-5, pd.np.nan, inplace=True)

        # ids replace
        for col in data.keys():
            df[col] = df[col].replace(dict(zip(data[col]['prev_id'], data[col]['id'].astype('int'))))

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
            connector=["housing-data-2020", "labels"],
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