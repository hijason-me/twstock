FROM python:3.11-slim

WORKDIR /app

# Install uv for fast dependency install
RUN pip install --no-cache-dir uv

COPY pyproject.toml .
RUN uv pip install --system --no-cache .

COPY src/ src/

ENTRYPOINT ["python", "-m", "twstock"]
CMD ["--help"]
