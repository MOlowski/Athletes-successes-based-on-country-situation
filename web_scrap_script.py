import requests
from bs4 import BeautifulSoup
import pandas as pd
import os

# script sending datasets to postgresql database

# getting data from websites

url = 'https://www.worldometers.info/world-population/population-by-country/'
response = requests.get(url)


if response.status_code == 200:
    soup = BeautifulSoup(response.text, 'html.parser')
    print('correct')
else:
    print(f"Error: {response.status_code}")
## finding table

table = soup.find('table', id='example2')

## extract data from table

rows = table.find_all('tr')

column_names_row = rows[0]
column_names = [col.text.strip() for col in column_names_row.find_all('th')]

data = []
for row in rows[1:]:
    row_el =row.find_all('td')
    row_data = [el.text.strip() for el in row_el]
    data.append(row_data)

population_per_country = pd.DataFrame(data, columns = column_names)

# save data to csv file
filename = 'datasets/population_per_country.csv'
if os.path.exists(filename):
    os.remove(filename)

population_per_country.to_csv('datasets/population_per_country.csv', index=False)