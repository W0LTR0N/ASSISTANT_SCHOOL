FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libssl-dev \
    libffi-dev \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd -r woltron && useradd -r -g woltron woltron

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p /app/data && chown -R woltron:woltron /app/data

USER woltron

EXPOSE 5060/udp
EXPOSE 10000-10100/udp

HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
    CMD python -c "import os; exit(0 if os.path.exists('/app/data/heartbeat.txt') else 1)"

CMD ["python", "-m", "app.main"]
