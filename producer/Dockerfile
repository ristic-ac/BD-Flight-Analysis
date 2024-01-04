FROM python:3.7-slim

WORKDIR /usr/src/app

COPY . .
RUN pip install --no-cache-dir -r requirements.txt

ENV KAFKA_BROKER="localhost:9092"
# TODO: change topic name
ENV TOPIC="flights_germany"


CMD ["python", "-u", "./main.py"]