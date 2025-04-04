# Use Python 3.12 slim image as base with specific version
FROM python:3.12-slim@sha256:a866731a6b71c4a194a845d86e06568725e430ed21821d0

# Set environment variables
ENV POETRY_HOME=/home/appuser/.poetry \
    POETRY_VERSION=1.7.1 \
    PATH="${POETRY_HOME}/bin:${PATH}" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# Create non-root user and set up directories
RUN useradd -m -u 1000 appuser && \
    mkdir -p /home/appuser/.config /home/appuser/.cache/pypoetry/virtualenvs && \
    chown -R appuser:appuser /home/appuser

# Set working directory and switch to non-root user
WORKDIR /app
USER appuser

# Install system dependencies and poetry
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        libgl1-mesa-glx \
        libglib2.0-0 \
        curl \
    && rm -rf /var/lib/apt/lists/* \
    && curl -sSL https://install.python-poetry.org | python3 - \
    && poetry config virtualenvs.create false

# Copy poetry files and install dependencies
COPY --chown=appuser:appuser api/pyproject.toml api/poetry.lock ./
RUN poetry install --no-interaction --no-ansi --no-root

# Copy application code and install
COPY --chown=appuser:appuser api/ api/
RUN poetry install --no-interaction --no-ansi

# Expose port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Command to run the application
CMD ["python", "-m", "uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]
