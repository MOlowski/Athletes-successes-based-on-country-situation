FROM python:3.10

WORKDIR /athletes_successes

COPY . /athletes_successes

RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 8888

CMD ["bash", "./entrypoint.sh"]