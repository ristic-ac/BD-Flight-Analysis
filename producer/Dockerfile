FROM python:3.7-slim

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

ENV KAFKA_BROKER="localhost:9092"
ENV TOPIC="flights_germany"


CMD ["python", "-u", "./main.py"]