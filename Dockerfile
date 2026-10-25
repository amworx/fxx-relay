FROM python:3.13-slim

WORKDIR /app

# Postgres driver for the cloud relay. SQLite needs nothing beyond the stdlib,
# so this is the only dependency the image adds.
RUN pip install --no-cache-dir "psycopg[binary]==3.2.3"

COPY *.py ./

EXPOSE 8787

CMD ["python", "main.py"]
