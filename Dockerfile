FROM python:3.14.0-slim-bookworm
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/


WORKDIR /app

COPY main.py .
COPY *.toml .
COPY uv.lock .

# Directory to mount volume
RUN mkdir -p /app/downloads

CMD [ "uv", "run", "main.py" ]