import psycopg2
import pandas as pd
from psycopg2 import Error
import os

csv_files = [
    'athlete_events',
    'noc_regions', 
    'gdp',
    'healthcare_expenditure_gdp',
    'obesity_adults',
    'political_regime',
    'poverty',
    'population']

pgconn = psycopg2.connect(
    host = 'postgres',
    user = 'postgres',
    port = '5432',
    password = 'pass')

def fetch_data(table_name, pgconn):
    q = f"SELECT * FROM {table_name}"
    return pd.read_sql_query(q, conn)




dataframes = {fetch_data(file) for file in csv_files}

        


df = dataframes['athlete_events'].merge(dataframes['noc_regions'], left_on='NOC', right_on='NOC')
df.rename(columns={'region':'Entity'})


dataframes['gdp'].rename(columns={'GDP per capita':'GDP'}, inplace=True)
dataframes['gdp'] = dataframes['gdp'][['Entity','Year','GDP']]

dataframes['healthcare_expenditure_gdp'] .rename(columns= {'Current health expenditure (CHE) as percentage of gross domestic product (GDP) (%)':'Health exp'}, inplace=True)
dataframes['healthcare_expenditure_gdp'] .drop(columns={'Code'}, inplace=True)

dataframes['obesity_adults'].rename(columns= {'Prevalence of obesity among adults, BMI >= 30 (crude estimate) (%) - Sex: both sexes - Age group: 18+  years':'Obesity'}, inplace=True)
dataframes['obesity_adults'].drop(columns={'Code'}, inplace=True)

dataframes['political_regime'].drop(columns={'Code'}, inplace=True)
mapping = {0: 'closed autocracy', 1: 'electoral autocracy', 2: 'electoral democracy', 3: 'liberal democracy'}
dataframes['political_regime']['Political regime'] = dataframes['political_regime']['Political regime'].replace(mapping)

dataframes['poverty'].rename(columns={'Contry':'Entity','Share below $1 a day':'1$', 'Share below $2.15 a day':'2.15$', 'Share below $3.65 a day':'3.65$', 'Share below $10 a day':'10$'}, inplace= True)
dataframes['poverty'] = dataframes['poverty'][['Entity', 'Year', '1$', '2.15$', '3.65$', '10$']]

dataframes['population'].drop(columns={'Code'}, inplace=True)
dataframes['population'].rename(columns={'Population (historical estimates)':'Population'}, inplace=True)
dataframes['population']['Population_20_y_bef'] = dataframes['population'].groupby('Entity')['Population'].transform(lambda x: x.shift(3))

dataframes['population'] = dataframes['population'].loc[dataframes['population']['Year']>1890]

def df_merging(dfs):
    merged_df = dfs[1]
    dfs = dfs[2:]
    for df in dfs:
        merged_df = merged_df.merge(df, how='outer', on=['Entity','Year'])
    return merged_df

countries_situation = df_merging(dataframes)

insert_table_q = ""

save_df_q = ''