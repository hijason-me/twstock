FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source
COPY src/ ./src/

ENV PYTHONPATH=/app

# Default job is passed as CMD argument in K8s CronJob spec
ENTRYPOINT ["python", "-m", "src.main"]
CMD ["--job", "daily_prices"]
