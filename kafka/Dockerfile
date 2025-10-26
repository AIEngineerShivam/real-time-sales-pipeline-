FROM python:3.11-slim

WORKDIR /app

# Install dependencies
RUN pip install --no-cache-dir \
    kafka-python==2.0.2 \
    requests==2.31.0 \
    psycopg2-binary==2.9.9

COPY . /app

CMD ["python", "producer.py"]