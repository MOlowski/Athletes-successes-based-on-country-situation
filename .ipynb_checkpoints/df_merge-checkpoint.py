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
try:
        
    pgconn = psycopg2.connect(
        host = 'postgres',
        user = 'postgres',
        password = 'pass',
        database = 'athletes_successes')

    pgcursor = pgconn.cursor()

    def fetch_data(table_name):
        q = f"SELECT * FROM {table_name}"
        return pd.read_sql_query(q, pgconn)

    dataframes = {file:fetch_data(file) for file in csv_files}

    df = dataframes['athlete_events'].merge(dataframes['noc_regions'], left_on='noc', right_on='noc')
    df.rename(columns={'region':'entity'})

    del dataframes['athlete_events']
    del dataframes['noc_regions']

    dataframes['gdp'].rename(columns={'gdp_per_capita':'gdp'}, inplace=True)
    dataframes['gdp'] = dataframes['gdp'][['entity','year','gdp']]

    dataframes['healthcare_expenditure_gdp'].rename(columns= {'current_health_expenditure__che__as_percentage_of_gross_domesti':'health_exp'}, inplace=True)
    dataframes['healthcare_expenditure_gdp'].drop(columns={'code'}, inplace=True)

    dataframes['obesity_adults'].rename(columns= {'prevalence_of_obesity_among_adults__bmi____30__crude_estimate__':'obesity'}, inplace=True)
    dataframes['obesity_adults'].drop(columns={'code'}, inplace=True)

    dataframes['political_regime'].drop(columns={'code'}, inplace=True)
    mapping = {0: 'closed_autocracy', 1: 'electoral_autocracy', 2: 'electoral_democracy', 3: 'liberal_democracy'}
    dataframes['political_regime']['politica_regime'] = dataframes['political_regime']['political_regime'].replace(mapping)

    dataframes['poverty'].rename(columns={'country':'entity','share_below__1_a_day':'one_dollar', 'share_below__2_15_a_day':'two_dollars', 'share_below__3_65_a_day':'four_dollars', 'share_below__10_a_day':'ten_dollars'}, inplace= True)
    dataframes['poverty'] = dataframes['poverty'][['entity', 'year', 'one_dollar', 'two_dollars', 'four_dollars', 'ten_dollars']]

    dataframes['population'].drop(columns={'code'}, inplace=True)
    dataframes['population'].rename(columns={'population__historical_estimates_':'population'}, inplace=True)
    dataframes['population']['population_20_y_bef'] = dataframes['population'].groupby('entity')['population'].transform(lambda x: x.shift(3))

    dataframes['population'] = dataframes['population'].loc[dataframes['population']['year']>1890]

    def df_merging(dfs):

        for key in dfs.keys():
            merged_df = dfs[key]
            del dfs[key]
            break
            
        for key in dfs.keys():
            merged_df = merged_df.merge(dfs[key], how='outer', on=['entity','year'])
        return merged_df

    countries_situation = df_merging(dataframes)

    columns = []

    for column, dtype in countries_situation.dtypes.items():
        sql_type = ''
        if dtype == 'int64':
            sql_type = 'INT'
        elif dtype == 'float64':
            sql_type = 'FLOAT'
        else:
            sql_type = 'VARCHAR(255)'
        columns.append(f"{column} {sql_type}")

    create_table_q = f"""
        CREATE TABLE countries_df(
            {', '.join(columns)}
        )
    """

    pgcursor.execute("""DROP table IF EXISTS countries_df""")
    pgcursor.execute(create_table_q)
    print("table countries_df created successfully")
    pgconn.commit()


    ### insert data into merged_df

    insert_table_q = """
        INSERT INTO countries_df ({})
        VALUES ({})
    """.format(','.join(countries_situation.columns),','.join(['%s']*len(countries_situation.columns)))

    pgcursor.executemany(insert_table_q, countries_situation.values.tolist())
    pgconn.commit()
    
except psycopg2.OperationalError as e:
    print(f'errror occured {e}')

finally:
    if pgconn:
        pgcursor.close()
        pgconn.close()
    print('script worked properly')