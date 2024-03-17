FROM python:latest

WORKDIR /athletes_successes

COPY . /athletes_successes

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "web_scrap_script.py"]