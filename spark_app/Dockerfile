FROM apache/spark:3.5.0

USER root

WORKDIR /app

COPY spark_app.py .
COPY requirements.txt .

RUN pip3 install --no-cache-dir -r requirements.txt
