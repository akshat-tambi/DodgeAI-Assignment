FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends mariadb-server mariadb-client \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY start.sh ./start.sh

RUN chmod +x /app/start.sh \
    && mkdir -p /var/data/mysql /app/tmp_uploads

EXPOSE 8000

CMD ["/app/start.sh"]
