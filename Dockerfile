FROM python:3.13-slim

WORKDIR /app

COPY *.py ./

EXPOSE 8787

CMD ["python", "main.py"]
