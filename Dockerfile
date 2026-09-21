# Image utilisée par Kestra (runner Docker) pour exécuter les tâches Python
# du pipeline avec les mêmes dépendances que le reste du projet.

FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim

WORKDIR /app

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-install-project

COPY src ./src

ENV PATH="/app/.venv/bin:${PATH}"
