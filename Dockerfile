FROM python:3.10

WORKDIR /athletes_successes

COPY . /athletes_successes

RUN pip install --no-cache-dir -r requirements.txt

RUN python web_scrap_script.py && python data_to_sql_script.py

EXPOSE 8888

CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--allow-root", "--no-browser"]