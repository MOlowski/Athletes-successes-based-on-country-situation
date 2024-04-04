#!/bin/bash

# Run web scrap script
python web_scrap_script.py

# Run data to SQL script
python data_to_sql_script.py

jupyter lab --ip=0.0.0.0 --port=8888 --allow-root --no-browser