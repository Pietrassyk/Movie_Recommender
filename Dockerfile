FROM protonai/pyspark

COPY ./requirements.txt /
ADD flask /usr/local/flask

RUN pip install -r requirements.txt

CMD cd /usr/local/flask && python3 webhook.py