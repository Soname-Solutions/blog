FROM python:3.10

WORKDIR /app
RUN mkdir -p file_source_zone

COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . /app/.

ENTRYPOINT ["/app/entrypoint.sh"]