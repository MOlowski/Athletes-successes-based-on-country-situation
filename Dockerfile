FROM python:3.10

WORKDIR /athletes_successes

COPY . /athletes_successes

RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 8888

# Copy entrypoint script
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh
RUN chmod +x web_scrap_script.py data_to_sql_script.py df_merge.py
# Set the entrypoint script as the entry point
ENTRYPOINT ["entrypoint.sh"]