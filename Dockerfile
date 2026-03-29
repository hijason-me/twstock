FROM python:3.11-slim

WORKDIR /app

# Install uv for fast dependency install
RUN pip install --no-cache-dir uv

# Copy source BEFORE installing so hatchling can find the package
COPY pyproject.toml .
COPY src/ src/

RUN uv pip install --system --no-cache .

ENTRYPOINT ["python", "-m", "twstock"]
CMD ["--help"]
