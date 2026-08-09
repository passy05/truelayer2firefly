FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV POETRY_VERSION=2.1.3
ENV PATH="/home/appuser/.local/bin:$PATH"

# Install curl (required for poetry installation) and clean up
RUN apt-get update && apt-get install -y curl \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Create a non-root user for security
RUN useradd -m appuser
USER appuser
WORKDIR /home/appuser/app

# Install Poetry as the non-root user
RUN curl -sSL https://install.python-poetry.org | python3 -

# Copy dependency files first for optimal layer caching
COPY --chown=appuser:appuser pyproject.toml poetry.lock ./
RUN poetry install --no-interaction --no-ansi --only main --no-root

# Copy application source code
COPY --chown=appuser:appuser . .

# Run the app natively via the python entry point
CMD ["poetry", "run", "python", "truelayer2firefly.py"]

