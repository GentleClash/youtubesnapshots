FROM python:3.13-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    ffmpeg \
    curl \
    wget \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean 

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .
COPY gcscache.py .
COPY localcache.py .


RUN groupadd -r appuser && useradd -r -g appuser -s /bin/false -M appuser && \
    mkdir -p /tmp/screenshots && \
    chown -R appuser:appuser /app /tmp/screenshots 

ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

USER appuser

EXPOSE 8000

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
