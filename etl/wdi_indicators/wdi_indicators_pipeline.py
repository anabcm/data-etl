
import pandas as pd
from datetime import datetime
from bamboo_lib.connectors.models import Connector
from bamboo_lib.helpers import query_to_df
from bamboo_lib.models import EasyPipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import DownloadStep
from bamboo_lib.steps import LoadStep


CORRECTIONS = {
    'css': 'xxa',
    'xkx': 'ksv',
    'sxm': 'maf',
}

EXCLUSIONS = [
    'arb', 'ceb', 'ear', 'eas', 'eap', 'tea', 'emu', 'ecs', 'eca', 'tec', 'euu',
    'fcs', 'hpc', 'hic', 'ibd', 'ibt', 'idb', 'idx', 'ida', 'lte', 'lcn', 'lac',
    'tla', 'ldc', 'lmy', 'lic', 'lmc', 'mea', 'mna', 'tmn', 'mic', 'nac', 'inx',
    'oed', 'oss', 'pss', 'pst', 'pre', 'sst', 'sas', 'tsa', 'ssf', 'ssa', 'tss',
    'umc',
]


class ExtractStep(PipelineStep):
    def run_step(self, prev, params):
        df = pd.read_csv(prev[0])

        df.rename(columns={'Country Code': 'geo_id', 'Indicator Code': 'indicator_id'}, inplace=True)

        df['geo_id'] = df['geo_id'].str.lower()

        # Remove rows with geo_id in EXCLUSIONS
        # These are organizations that we don't want to show on the front-end
        # as countries
        df = df[~df['geo_id'].isin(EXCLUSIONS)]

        df['geo_id'].replace(CORRECTIONS, inplace=True)

        id_vars = ['geo_id', 'indicator_id']
        value_vars = [str(i) for i in range(1960, 2019 + 1)]

        df = pd.melt(df, id_vars=id_vars, value_vars=value_vars)

        df.rename(columns={'variable': 'year', 'value': 'measure'}, inplace=True)

        df = df.dropna(subset=['measure'])

        df['year'] = df['year'].astype(int)

        # Add extra indicators from spreadsheet
        extra_df = pd.read_csv(prev[1])
        extra_data = []

        for i, row in extra_df.iterrows():
            shared_fields = {
                'geo_id': 'twn',
                'year': row['year'],
            }

            total_pop = int(row['pop_combined'].replace(' ', '')) * 1000
            total_female_pop = int(row['pop_female'].replace(' ', '')) * 1000
            total_male_pop = int(row['pop_male'].replace(' ', '')) * 1000
            pct_female_pop = (total_female_pop / float(total_pop)) * 100
            pct_male_pop = (total_male_pop / float(total_pop)) * 100

            extra_data.append({**shared_fields, **{'indicator_id': 'SP.POP.TOTL', 'measure': total_pop}})
            extra_data.append({**shared_fields, **{'indicator_id': 'SP.POP.TOTL.FE.IN', 'measure': total_female_pop}})
            extra_data.append({**shared_fields, **{'indicator_id': 'SP.POP.TOTL.FE.ZS', 'measure': pct_female_pop}})
            extra_data.append({**shared_fields, **{'indicator_id': 'SP.POP.TOTL.MA.IN', 'measure': total_male_pop}})
            extra_data.append({**shared_fields, **{'indicator_id': 'SP.POP.TOTL.MA.ZS', 'measure': pct_male_pop}})

        extra_df = pd.DataFrame(extra_data)

        # Merge the original dataframe with the extra data
        df = pd.concat([df, extra_df], sort=False)

        df['version'] = datetime.now()
        df['version'] = pd.to_datetime(df['version'], infer_datetime_format=True)

        return df


class WDIAnnualIndicatorsPipeline(EasyPipeline):
    @staticmethod
    def steps(params):
        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))

        dtype = {
            'geo_id':                  'String',
            'indicator_id':            'String',
            'year':                    'UInt16',
            'measure':                 'Float64',
            'version':                 'DateTime',
        }

        download_step = DownloadStep(
            connector=["wdi-data", "wdi-extra-indicators"],
            connector_path="conns.yaml"
        )
        extract_step = ExtractStep(connector=db_connector)
        load_step = LoadStep(
            "indicators_i_wdi_a", db_connector, if_exists="drop", dtype=dtype,
            pk=['geo_id', 'indicator_id', 'year'],
            engine="ReplacingMergeTree", engine_params='version'
        )

        return [download_step, extract_step, load_step]

if __name__ == '__main__':
    pp = WDIAnnualIndicatorsPipeline()
    pp.run({})