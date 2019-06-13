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
        db_connector = Connector.fetch("clickhouse-database", open("etl/conns.yaml"))
        dtype = {
            "class_id":     "String",
            "mun_id":       "UInt16",
            "year":         "UInt8",
            "ue":           "Float64",
            "a111a":        "UFloat32",
            "h001a":        "UInt16",
            "h000a":        "Float64",
            "h010a":        "UInt16",
            "h020a":        "UInt16",
            "i000a":        "Float64",
            "j000a":        "Float64",
            "k000a":        "Float64",
            "m000a":        "Float64",
            "a111a":        "Float64",
            "a121a":        "Float64",
            "a131a":        "Float64",
            "a211a":        "Float64",
            "a221a":        "Float64",
            "p000c":        "Float64",
            "q000a":        "Float64",
            "q000b":        "Float64",
            "a700a":        "Float64",
            "a800a":        "Float64",
            "q000c":        "Float64",
            "q000d":        "Float64",
            "p000a":        "Float64",
            "p000b":        "Float64",
            "o010a":        "Float64",
            "o020a":        "Float64",
            "m700a":        "Float64",
            "p030c":        "Float64",
            "a511a":        "Float64",
            "m020a":        "Float64",
            "m050a":        "Float64",
            "m091a":        "Float64",
            "h001b":        "UInt16",
            "h001c":        "UInt16",
            "h001d":        "Float64",
            "h000b":        "Float64",
            "h000c":        "Float64",
            "h000d":        "Float64",
            "h010b":        "UInt16",
            "h010c":        "UInt16",
            "h010d":        "Float64",
            "h101a":        "Float64",
            "h101b":        "Float64",
            "h101c":        "Float64",
            "h101d":        "Float64",
            "h203a":        "Float64",
            "h203b":        "Float64",
            "h203c":        "Float64",
            "h203d":        "Float64",
            "h020b":        "UInt16",
            "h020c":        "UInt16",
            "h020d":        "Float64",
            "i000b":        "Float64",
            "i000c":        "Float64",
            "i000d":        "Float64",
            "i100a":        "Float64",
            "i100b":        "Float64",
            "i100c":        "Float64",
            "i100d":        "Float64",
            "i200a":        "UInt16",
            "i200b":        "UInt16",
            "i200c":        "UInt16",
            "i200d":        "Float64",
            "j010a":        "Float64",
            "j203a":        "Float64",
            "j300a":        "Float64",
            "j400a":        "Float64",
            "j500a":        "Float64",
            "j600a":        "Float64",
            "k010a":        "Float64",
            "k020a":        "Float64",
            "k030a":        "Float64",
            "k311a":        "Float64",
            "k040a":        "Float64",
            "k041a":        "Float64",
            "k050a":        "Float64",
            "k610a":        "Float64",
            "k620a":        "Float64",
            "k060a":        "Float64",
            "k070a":        "Float64",
            "k810a":        "Float64",
            "k820a":        "Float64",
            "k910a":        "Float64",
            "k950a":        "Float64",
            "k096a":        "Float64",
            "k976a":        "Float64",
            "k090a":        "Float64",
            "m010a":        "Float64",
            "m030a":        "Float64",
            "m090a":        "Float64",
            "p100a":        "Float64",
            "p100b":        "Float64",
            "p030a":        "Float64",
            "p030b":        "Float64",
            "q010a":        "Float64",
            "q020a":        "Float64",
            "q030a":        "Float64",
            "q400a":        "Float64",
            "q900a":        "Float64",
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