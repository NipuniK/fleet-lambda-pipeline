FROM apache/spark:3.5.0
USER root
WORKDIR /app
COPY processing/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY processing/ .
COPY common/ ./common
