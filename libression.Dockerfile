# Use Python 3.12 slim image as base with specific version
FROM python:3.12-slim

# Set environment variables
ENV POETRY_HOME=/home/appuser/.poetry \
    POETRY_VERSION=1.7.1 \
    PATH="/home/appuser/.poetry/bin:${PATH}" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app \
    DB_PATH=/app/data/libression.db

# Create non-root user and set up directories
RUN useradd -m -u 1000 appuser && \
    mkdir -p /home/appuser/.config /home/appuser/.cache/pypoetry/virtualenvs && \
    mkdir -p /app/data && \
    chown -R appuser:appuser /home/appuser /app/data

# Set working directory
WORKDIR /app

# Install system dependencies and poetry as root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        libgl1-mesa-glx \
        libglib2.0-0 \
        curl \
    && rm -rf /var/lib/apt/lists/* \
    && curl -sSL https://install.python-poetry.org | python3 - \
    && poetry config virtualenvs.create false

# Switch to non-root user
USER appuser

# Copy all application files and install
COPY --chown=appuser:appuser api/ ./
RUN poetry install --no-interaction --no-ansi

# Expose port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Command to run the application
CMD ["poetry", "run", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
