
def format_text(df, cols_names=None, stopwords=None):
    # format
    for ele in cols_names:
        df[ele] = df[ele].str.title().str.strip()
        for ene in stopwords:
            df[ele] = df[ele].str.replace(' ' + ene.title() + ' ', ' ' + ene + ' ')

    return df

def fill_values(df, target, base):
    """fill nan values with another column where there are values"""
    mask = df[target].isnull()
    df.loc[mask, target] = df.loc[mask, base]
    return df

import nltk
import pandas as pd
from bamboo_lib.models import PipelineStep
from bamboo_lib.models import EasyPipeline
from bamboo_lib.connectors.models import Connector
from bamboo_lib.steps import LoadStep
from sklearn.feature_extraction import stop_words

class ReadStep(PipelineStep):
    def run_step(self, prev, params):
        url = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vQ2O8Z8QIAQ51JPvXSXqBLZkYpu0CrvAhj40Mz7gHsLsdtCRo4zB4ielNOZh6v28DEnNpbdFyu33o0J/pub?output=xlsx'
        sector = pd.read_excel(url, sheet_name='sector')
        subsector = pd.read_excel(url, sheet_name='subsector')
        industry_group = pd.read_excel(url, sheet_name='industry_group')
        naics_industry = pd.read_excel(url, sheet_name='naics_industry')
        national_industry = pd.read_excel(url, sheet_name='national_industry')
        return sector, subsector, industry_group, naics_industry, national_industry

class CleanStep(PipelineStep):
    def run_step(self, prev, params):
        sector, subsector, industry_group, naics_industry, national_industry = prev[0], prev[1], prev[2], prev[3], prev[4]

        fill_values(sector, 'sector_es_short', 'sector_es')
        fill_values(sector, 'sector_en_short', 'sector_en')
        fill_values(subsector, 'subsector_es_short', 'subsector_es')
        fill_values(subsector, 'subsector_en_short', 'subsector_en')
        fill_values(industry_group, 'industry_group_es_short', 'industry_group_es')
        fill_values(industry_group, 'industry_group_en_short', 'industry_group_en')
        fill_values(naics_industry, 'naics_industry_es_short', 'naics_industry_es')
        fill_values(naics_industry, 'naics_industry_en_short', 'naics_industry_en')
        fill_values(national_industry, 'national_industry_es_short', 'national_industry_es')
        fill_values(national_industry, 'national_industry_en_short', 'national_industry_en')

        data = {'sector': sector,
                'subsector': subsector, 
                'industry': industry_group, 
                'naics_industry': naics_industry}
        cols = {}

        for k, table in data.items():
            #temp = list(table.columns[(table.columns != 'value') & (~table.columns.str.contains('id'))])
            temp = list(table.columns[(table.columns != 'value')])
            cols[k] = {'len': len(temp), 
                    'digits': len(str(table.loc[0, table.columns[table.columns.str.contains('id')]].max())),
                    'cols': temp,
                    'id_col': table.columns[table.columns.str.contains('id')][0],
                    'data': table}

        for k, v in cols.items():
            for le in range(v['len']):
                national_industry[v['cols'][le]] = national_industry['national_industry_id'].astype('str').str[:v['digits']]
                national_industry[v['cols'][le]].replace(dict(zip(v['data'][v['id_col']].astype('str'), v['data'][v['cols'][le]])), inplace=True)
                    

        for col in national_industry.columns:
            national_industry[col] = national_industry[col].astype('str')

        for col in national_industry.loc[:, sector.columns].columns:
            for col_ in sector.columns:
                if col == col_:
                    #print(col)
                    sector[col_] = sector[col_].astype('str')
                    national_industry[col].replace('31', '31-33', inplace=True)
                    national_industry[col].replace('32', '31-33', inplace=True)
                    national_industry[col].replace('33', '31-33', inplace=True)
                    national_industry[col].replace('48', '48-49', inplace=True)
                    national_industry[col].replace('49', '48-49', inplace=True)
                    national_industry[col].replace(dict(zip(sector['sector_id'].astype('str'), sector[col_])), inplace=True)
                    break

        national_industry.sort_values(by='national_industry_id', inplace=True)

        cols_es = list(national_industry.columns[national_industry.columns.str.contains('_es')])
        cols_en = list(national_industry.columns[national_industry.columns.str.contains('_en')])

        # codes ids
        nltk.download('stopwords')
        stopwords_es = nltk.corpus.stopwords.words('spanish')
        national_industry = format_text(national_industry, cols_es, stopwords=stopwords_es)
        national_industry = format_text(national_industry, cols_en, stopwords=stop_words.ENGLISH_STOP_WORDS)

        for col in list(national_industry.columns[national_industry.columns.str.contains('id')]):
            if 'sector' not in col:
                national_industry[col] = national_industry[col].astype('str')

        national_industry.drop_duplicates(subset='national_industry_id', inplace=True)

        national_industry.drop(columns='value', inplace=True)

        national_industry = national_industry.groupby(['sector_id', 'sector_es', 'sector_es_short', 'sector_en', 'sector_en_short', 
            'subsector_id', 'subsector_es', 'subsector_es_short', 'subsector_en', 'subsector_en_short', 
            'industry_group_id', 'industry_group_es', 'industry_group_es_short', 'industry_group_en',  'industry_group_en_short']).sum().reset_index(col_fill='ffill')

        return national_industry

class IndustryPipeline(EasyPipeline):
    @staticmethod
    def steps(params, **kwargs):
        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))

        read_step = ReadStep()
        clean_step = CleanStep()
        load_step = LoadStep('dim_shared_industry_enoe', db_connector, if_exists='drop', pk=['industry_group_id', 'subsector_id', 'sector_id'])
        
        return [read_step, clean_step, load_step]
