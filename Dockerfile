FROM python:3.10

WORKDIR /app/

COPY requirements.txt requirements.txt
COPY . .

RUN python3 -m pip install -r requirements.txt
WORKDIR /app/danelfin_demo

ENTRYPOINT ["python", "./main.py"]
