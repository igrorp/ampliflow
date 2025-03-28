FROM python:3.10-slim

WORKDIR /app

COPY . .

RUN pip install . --no-cache-dir -r requirements.txt

# ENTRYPOINT ["python", "src/download_fastqs/download_fastqs.py"]
# CMD ["-h"]