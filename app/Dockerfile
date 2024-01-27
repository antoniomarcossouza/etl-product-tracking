FROM python:3.11-slim-bookworm

WORKDIR /

RUN apt update && apt install libpq-dev gcc -y

COPY . /app

RUN pip install -r app/requirements.txt

CMD ["python", "./app/main.py"]