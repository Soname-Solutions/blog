FROM python:3.10

WORKDIR /app

COPY requierements.txt .

RUN pip install -r requierements.txt

COPY . .

CMD ["python", "ask_spotify.py"]