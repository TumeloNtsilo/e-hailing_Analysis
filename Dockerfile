FROM python:3.12

WORKDIR /pipeline

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN apt-get update && apt-get install -y ca-certificates

CMD ["python3", "main.py"]