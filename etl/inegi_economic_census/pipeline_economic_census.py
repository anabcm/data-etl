import pandas as pd
from bamboo_lib.models import PipelineStep, AdvancedPipelineExecutor
from bamboo_lib.models import Parameter, BasePipeline
from bamboo_lib.connectors.models import Connector
from bamboo_lib.steps import LoadStep
from bamboo_lib.helpers import grab_connector

class MultiStep(PipelineStep):
    def run_step(self, prev, params):
        df = pd.read_csv(
            "https://storage.googleapis.com/datamexico-data/inegi_economic_census/economic_census.csv", 
            encoding="latin-1", 
            low_memory=False)

        # Drops `Total` rows and empty columns
        df.drop(["Unnamed: 118"], axis=1, inplace=True)
        df.drop(df.loc[df["Actividad Económica"].str.match("Total nacional") | df["Actividad Económica"].str.match("Total estatal") | df["Actividad Económica"].str.match("Total municipal")].index, inplace=True)
        df.drop(df[df["Entidad"]=="00 Total Nacional"].index, inplace=True)
        df = df[~df["Año Censal"].isnull()]
        df = df[pd.notnull(df["Municipio"])]
        
        #  Renames columns, keeping measure code
        temp = list(df.columns[:4])
        for i in range(4, len(df.columns)):
            temp.append(df.columns[i].split(" ")[0].lower())
        df.columns = temp

        # Creates mun_id
        piv_ent = df["Entidad"].str.split(" ", n=1, expand=True)
        piv_mun = df["Municipio"].str.split(" ", n=1, expand=True)
        df["Entidad"] = piv_ent[0] + piv_mun[0]

        # Updates column names
        params = {
          "Año Censal":"year",
          "Entidad":"mun_id",
          "Actividad Económica":"class_id"
        }
        df.rename(index=str, columns=params, inplace=True)

        # Deletes duplicates columns
        df = df.loc[:, ~df.columns.duplicated()]

        # Keeps class_id
        piv_class = df["class_id"].str.split(" ", n=1, expand=True)
        df["class_id"] = piv_class[0]

        # Deletes Municio column
        df.drop(["Municipio"], axis=1, inplace=True)

        # Converts mun_id and year columns to integer variables
        df["mun_id"] = df["mun_id"].astype(int)
        df["year"] = df["year"].astype(int)

        # Reset indexes
        df.drop(df.columns[[-1,-2,-3]], axis=1, inplace=True)
        df.reset_index(drop=True, inplace=True)

        return df

class EconomicCensusPipeline(BasePipeline):
    @staticmethod
    def pipeline_id():
        return "pipeline_economic_census"

    @staticmethod
    def name():
        return "Economic Census Pipeline"

    @staticmethod
    def description():
        return "Dataframe for Mexican Economic Census"

    @staticmethod
    def website():
        return "https://www.inegi.org.mx/app/saic/default.aspx"

    @staticmethod
    def parameter_list():
        return [
            Parameter(label="Source connector", name="source-connector", dtype=str, source=Connector)
        ]

    @staticmethod
    def run(params, **kwargs):
        db_connector = Connector.fetch("clickhouse-database", open("etl/conns.yaml"))

        dtype = {
            'class_id':     'UInt16',
            'mun_id':       'UInt16',
            'year':         'UInt8',
            'ue':           'UInt32',
            'a111a':        'UFloat32'
        }

        # Definition of each step
        step1 = MultiStep()
        load_step = LoadStep(
            "inegi_economic_census", db_connector, if_exists="replace", pk=["class_id", "mun_id", "year"], dtype=dtype
        )

        # Definition of the pipeline and its steps
        pipeline = AdvancedPipelineExecutor(params)
        pipeline = pipeline.next(step1).next(load_step)
        return pipeline.run_pipeline()

def run_coverage(params, **kwargs):
    pipeline = EconomicCensusPipeline()
    pipeline.run(params)

if __name__ == "__main__":
    run_coverage({
        "source-connector": "http-local"
    })