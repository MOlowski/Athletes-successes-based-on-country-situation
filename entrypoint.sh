#!/bin/bash


if [ "$FORCE_SETUP" = "true" ]; then
    # Run setup script (replace with your actual setup command)
    python web_scrap_script.py

    # Run data to SQL script
    python data_to_sql_script.py
    python df_merge.py

    # Mark setup as completed
    touch /athletes_successes/setup_completed
else
    echo "FORCE_SETUP is not true. Skipping setup."
fi

exec jupyter lab --ip=0.0.0.0 --port=8888 --allow-root --no-browser