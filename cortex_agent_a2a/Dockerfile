FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY auth.py .
COPY executor.py .
COPY executor_spcs.py .
COPY main.py .

ENV PORT=8080
ENV HOSTNAME=0.0.0.0
ENV USE_SPCS=true

EXPOSE 8080

CMD ["python", "main.py"]
