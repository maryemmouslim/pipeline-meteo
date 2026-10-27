FROM python:3.12-slim
WORKDIR /opt/app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY src src
COPY tests tests
ENV PYTHONPATH=/opt/app

