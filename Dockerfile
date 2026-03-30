FROM python:3.11-slim

WORKDIR /app/config

# Install dependencies
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

CMD ["python", "/app/config/rescan.py"]
