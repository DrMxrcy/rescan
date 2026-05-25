FROM python:3.11-slim

WORKDIR /app

# Copy application files
COPY requirements.txt /app/requirements.txt
COPY rescan.py /app/rescan.py
COPY state_cache.py /app/state_cache.py

# Install dependencies
RUN pip install --no-cache-dir -r /app/requirements.txt

# Add healthcheck
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD python -c "import sys; sys.exit(0)" || exit 1

# Create non-root user
RUN useradd -u 1000 -m rescan
USER rescan

CMD ["python", "rescan.py"]
