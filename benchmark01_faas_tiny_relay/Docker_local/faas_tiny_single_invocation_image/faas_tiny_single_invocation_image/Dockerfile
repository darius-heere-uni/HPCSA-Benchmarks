FROM python:3.12-slim

WORKDIR /app
COPY faas_tiny_single_invocation.py /app/faas_tiny_single_invocation.py
COPY version.txt /app/version.txt

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

ENTRYPOINT ["python", "/app/faas_tiny_single_invocation.py"]
