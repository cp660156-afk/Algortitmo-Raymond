FROM python:3.12-slim
WORKDIR /app
COPY *.py ./
COPY config ./config
ENV PYTHONUNBUFFERED=1
