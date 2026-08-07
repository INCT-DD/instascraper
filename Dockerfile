FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml README.md requirements.txt ./
COPY instagram_scraper.py pipeline.py ./
COPY src ./src
COPY docs ./docs
COPY scripts ./scripts
COPY migrations ./migrations
COPY profiles.json ./profiles.json
COPY sessions.example.json ./sessions.example.json

RUN pip install --upgrade pip \
    && pip install -e .

CMD ["python", "-m", "pipeline", "--help"]
