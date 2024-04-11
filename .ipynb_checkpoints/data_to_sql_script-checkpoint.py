import psycopg2
import pandas as pd
from psycopg2 import Error
import os
print('check')
import time
print('check1')
#reading csv files
folder_path ='datasets/'
csv_files = [
    'athlete_events.csv', 
    'cities.csv', 
    'countries.csv',
    'gdp.csv',
    'noc_regions.csv',
    'political_regime.csv',
    'poverty.csv',
    'healthcare_expenditure_gdp.csv',
    'obesity_adults.csv',
    'population.csv']

dataframes = {}

for file in csv_files:
    file_name = os.path.splitext(os.path.basename(file))[0]
    dataframes[file_name] = pd.read_csv(os.path.join(folder_path, file))

def change_column_name(col):
    modified_char = ""
    for char in col:
        if char.isalnum() or char in ['_']:
            modified_char += char
        else:
            modified_char +='_'
    if modified_char[0].isdigit():
        modified_char = 'a' + modified_char
    return modified_char
    

for t_n, df in dataframes.items():
    df.rename(columns = change_column_name, inplace=True)
        
success = False
#creating database

pgconn = False

def is_postgres_available():
    try:
        # Try connecting to PostgreSQL
        conn = psycopg2.connect(
            host='postgres',
            user='postgres',
            port='5432',
            password='pass'
        )
        print('connected')
        conn.close()
        return True
    except psycopg2.OperationalError:
        return False

# Wait for PostgreSQL to be available
while not is_postgres_available():
    print("PostgreSQL is not available yet. Waiting...")
    time.sleep(1)

try:
    pgconn = psycopg2.connect(
        host = 'postgres',
        user = 'postgres',
        port = '5432',
        password = 'pass')
    
    with pgconn.cursor() as pgcursor:
        pgconn.autocommit = True
        pgcursor.execute('DROP DATABASE IF EXISTS athletes_successes')
        pgcursor.execute('CREATE DATABASE athletes_successes')
        print("created athletes_successes db")
    pgconn.close()

#creating tables 
    pgconn = psycopg2.connect(
        host = 'postgres',
        user = 'postgres',
        port = '5432',
        password = 'pass',
        database = 'athletes_successes')

    pgcursor = pgconn.cursor()
    
    for table_name, df in dataframes.items():
        columns = []
        if (table_name =='poverty') or (table_name =='population'):
            for column, dtype in df.dtypes.items():
                sql_type = ''
                if dtype == 'int64':
                    sql_type = 'BIGINT'
                elif dtype == 'float64':
                    sql_type = 'FLOAT'
                else:
                    sql_type = 'VARCHAR(255)'
                columns.append(f"{column} {sql_type}")

        else:
            for column, dtype in df.dtypes.items():
                sql_type = ''
                if dtype == 'int64':
                    sql_type = 'INT'
                elif dtype == 'float64':
                    sql_type = 'FLOAT'
                else:
                    sql_type = 'VARCHAR(255)'
                columns.append(f"{column} {sql_type}")

        create_table_q = f"""
            CREATE TABLE {table_name} (
                {', '.join(columns)}
            )
        """
        
        pgcursor.execute(create_table_q)
        print(f"table {table_name} created successfully")
    pgconn.commit()
#insert data into tables
    for table_name, df in dataframes.items():
        modified_columns = [change_column_name(col) for col in df.columns]
        insert_query = """
            INSERT INTO {} ({})
            VALUES ({})
        """.format(table_name, ','.join(modified_columns), ','.join(['%s']*len(df.columns)))
        pgcursor.executemany(insert_query, df.values.tolist())
        print(f'table {table_name} filled successfully')
    success = True
    pgconn.commit()
    

except Exception as error:
    print('Error while connecting to postgresql', error)

finally:
    if pgconn:
        pgcursor.close()
        pgconn.close()
        
if success:
    print('script sending data to postgresql worked correctly')