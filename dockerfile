FROM python:3.13-slim
ENV LANG=C.UTF-8
ENV LC_ALL=C.UTF-8
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY hwinfo_prom_exporter.py .

ENV LISTEN_HOST=0.0.0.0
ENV LISTEN_PORT=10445
ENV POLL_INTERVAL=5
ENV HTTP_TIMEOUT=2
ENV DOWN_RETRY_INTERVAL=2
ENV LOG_LEVEL=INFO

EXPOSE 10445

CMD ["python", "/app/hwinfo_prom_exporter.py"]