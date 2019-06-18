import pandas as pd
from bamboo_lib.models import PipelineStep, AdvancedPipelineExecutor
from bamboo_lib.models import Parameter, BasePipeline
from bamboo_lib.connectors.models import Connector
from bamboo_lib.steps import LoadStep, DownloadStep
from bamboo_lib.helpers import grab_connector

class MultiStep(PipelineStep):
    def run_step(self, prev, params):
        df = pd.read_csv(
            prev, 
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
            temp.append(df.columns[i].split(" ")[0])
        df.columns = temp

        # Creates mun_id
        piv_ent = df["Entidad"].str.split(" ", n=1, expand=True)
        piv_mun = df["Municipio"].str.split(" ", n=1, expand=True)
        df["Entidad"] = piv_ent[0] + piv_mun[0]

        # Updates column names
        params = {
          "Año Censal":"year",
          "Entidad":"mun_id",
          "Actividad Económica":"national_industry_id"
        }
        df.rename(index=str, columns=params, inplace=True)

        # Deletes duplicates columns
        df = df.loc[:, ~df.columns.duplicated()]

        # Keeps national_industry_id
        piv_class = df["national_industry_id"].str.split(" ", n=1, expand=True)
        df["national_industry_id"] = piv_class[0]

        # Deletes Municio column
        df.drop(["Municipio"], axis=1, inplace=True)

        # Converts mun_id and year columns to integer variables
        df["mun_id"] = df["mun_id"].astype(int)
        df["year"] = df["year"].astype(int)

        # Reset indexes and lowering columns headers
        df.drop(df.columns[[-1,-2,-3]], axis=1, inplace=True)
        df.columns = map(str.lower, df.columns)
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
        db_connector = Connector.fetch("clickhouse-database", open("../conns.yaml"))
        dl_step = DownloadStep(connector="dataset", connector_path=__file__)

        # Definition of each step
        transform_step = MultiStep()
        load_step = LoadStep(
            "inegi_economic_census", 
            db_connector, 
            if_exists="drop", 
            pk=["national_industry_id", "mun_id", "year"], 
            nullable_list=["m000a", "p000c", "a800a", "q000d", "p000a", "p000b", "p030c", "a511a", "m050a", "j203a", "j300a","j400a","j500a","j600a","k010a","k020a","k030a","k311a","k041a","k610a","k620a","k060a","k070a","k810a","k910a","k950a","k096a","k976a","m010a","m030a","m090a","p100a","p100b","p030a","p030b","q010a","q020a","q030a","q400a","q900a"],
            dtype={"mun_id": "UInt16"}
        )

        # Definition of the pipeline and its steps
        pipeline = AdvancedPipelineExecutor(params)
        pipeline = pipeline.next(dl_step).next(transform_step).next(load_step)
        return pipeline.run_pipeline()

def run_coverage(params, **kwargs):
    pipeline = EconomicCensusPipeline()
    pipeline.run(params)

if __name__ == "__main__":
    run_coverage({
        "source-connector": "http-local"
    })