FROM python:3.12-slim

WORKDIR /app

COPY faas_hybrid_etl_single_invocation.py /app/faas_hybrid_etl_single_invocation.py
COPY version.txt /app/version.txt

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV IMAGE_VERSION=faas-hybrid-etl-single-invocation-v1

CMD ["python", "/app/faas_hybrid_etl_single_invocation.py"]
