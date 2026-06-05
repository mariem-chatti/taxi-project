FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY producer.py .

# Use python directly — avoids needing /bin/sh (safer on Windows-built images)
ENTRYPOINT ["python", "-u", "producer.py"]
