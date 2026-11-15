FROM python:3.12-slim-bookworm

WORKDIR /srv
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY simulate.py ./

CMD ["python", "simulate.py"]
