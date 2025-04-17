FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS builder

WORKDIR /app

# Copy dependency files
COPY pyproject.toml README.md ./

# Install dependencies using uv
RUN --mount=type=cache,target=/root/.cache/uv \
    uv pip install --system -e .

# Copy source code
COPY ./src/db-migration /app/src
COPY ./src/db-migration/tests /app/tests

# Final stage
FROM python:3.12-slim-bookworm

WORKDIR /app

# Copy virtual environment and code from builder
COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=builder /app/src /app/src

# Create directory for migration files
RUN mkdir -p /app/migrations

# Set environment variables
ENV PYTHONPATH=/app

# Expose port
EXPOSE 8080

# Command to run the service
CMD ["python", "-m", "src.db-migration.main"]