FROM python:3.10

WORKDIR /athletes_successes

COPY . /athletes_successes

RUN pip install --no-cache-dir -r requirements.txt

CMD ["sh","-c", "python web_scrap_script.py && "python data_to_sql_script.py"]