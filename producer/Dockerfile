FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY producer_publish.py .

CMD ["python3", "producer_publish.py"]
